"""Host and container resources: memory, CPU, disk. Now, and over time.

Answers the questions a crash alone can't: was the VM out of memory when weather-stream
was OOM-killed, and WHO was using it? Is the stream behind because the CPU is saturated?

- snapshot(): one reading. Host numbers come from /proc (a container sees the host's
  /proc/meminfo and /proc/loadavg) and the disk holding the lake; per-container numbers
  from Docker's stats endpoint, through the same read-only socket proxy as the logs.
- History: the health monitor appends one snapshot per pass to RESOURCE_HISTORY
  (JSON lines, last 7 days), so the agent can look at memory BEFORE a crash.
- checks(): deterministic thresholds, shared by the real health checks and the evals.

Parsing and formatting are pure functions, so tests and the eval harness use them as-is.
"""
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

MEM_HIGH_PCT = 90          # host memory in use above this = pressure (the OOM killer is near)
DISK_HIGH_PCT = 85         # disk in use above this = soon full (Spark checkpoints, Kafka, lake)
LOAD_PER_CPU_HIGH = 1.5    # 1-min load above 1.5x the cores = CPU saturated, work queues up
AT_LIMIT_PCT = 90          # a container above this share of its own memory limit is about to be killed
HISTORY_DAYS = 7


# ---------------- parsing (pure) ----------------

def parse_meminfo(text: str) -> dict:
    kb = {}
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        parts = rest.split()
        if parts and parts[0].isdigit():
            kb[key.strip()] = int(parts[0])
    total = kb.get("MemTotal", 0) // 1024
    available = kb.get("MemAvailable", kb.get("MemFree", 0)) // 1024
    swap_total = kb.get("SwapTotal", 0) // 1024
    return {"mem_total_mb": total, "mem_available_mb": available,
            "swap_used_mb": swap_total - kb.get("SwapFree", 0) // 1024}


def parse_docker_stats(name: str, s: dict, host_total_mb: int | None) -> dict:
    """One container's /stats?stream=false response → memory (MB, excluding reclaimable
    page cache, like `docker stats`), its limit (None = no limit), CPU % and process count."""
    mem = s.get("memory_stats") or {}
    extra = mem.get("stats") or {}
    cache = extra.get("inactive_file", extra.get("total_inactive_file", extra.get("cache", 0)))
    used_mb = max(mem.get("usage", 0) - cache, 0) / 2**20
    limit_mb = (mem.get("limit") or 0) / 2**20
    # Docker reports the host's total memory as the "limit" of a container without one
    if not limit_mb or (host_total_mb and limit_mb >= host_total_mb * 0.95):
        limit_mb = None

    cpu, pre = s.get("cpu_stats") or {}, s.get("precpu_stats") or {}
    cpu_delta = (cpu.get("cpu_usage") or {}).get("total_usage", 0) - (pre.get("cpu_usage") or {}).get("total_usage", 0)
    sys_delta = cpu.get("system_cpu_usage", 0) - pre.get("system_cpu_usage", 0)
    cpus = cpu.get("online_cpus") or len((cpu.get("cpu_usage") or {}).get("percpu_usage") or []) or 1
    cpu_pct = cpu_delta / sys_delta * cpus * 100 if sys_delta > 0 and cpu_delta >= 0 else 0.0
    return {"name": name, "mem_mb": round(used_mb), "limit_mb": round(limit_mb) if limit_mb else None,
            "cpu_pct": round(cpu_pct, 1), "pids": (s.get("pids_stats") or {}).get("current")}


# ---------------- collecting (host + Docker) ----------------

def _host(disk_path: Path | None) -> dict:
    host = {"cpus": os.cpu_count() or 1}
    try:
        host.update(parse_meminfo(Path("/proc/meminfo").read_text()))
        load = Path("/proc/loadavg").read_text().split()
        host.update(load1=float(load[0]), load5=float(load[1]), load15=float(load[2]))
    except (OSError, ValueError, IndexError):
        pass
    if disk_path:
        du = shutil.disk_usage(disk_path)
        host.update(disk_used_pct=round(du.used / du.total * 100, 1), disk_free_gb=round(du.free / 2**30, 1))
    return host


def _container_stats(url: str, host_total_mb: int | None) -> list[dict]:
    with httpx.Client(base_url=url, timeout=10) as client:
        running = client.get("/containers/json").json()          # running containers only
    names = [c["Names"][0].lstrip("/") for c in running]

    # stream=false: Docker samples twice ~1 s apart so CPU % is meaningful; run them in parallel
    def one(name: str) -> dict | None:
        try:
            with httpx.Client(base_url=url, timeout=15) as client:
                resp = client.get(f"/containers/{name}/stats", params={"stream": "false"})
                resp.raise_for_status()
                return parse_docker_stats(name, resp.json(), host_total_mb)
        except (httpx.HTTPError, ValueError):
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        stats = [s for s in pool.map(one, names) if s]
    return sorted(stats, key=lambda c: -c["mem_mb"])


def snapshot(docker_url: str | None, disk_path: Path | None) -> dict:
    host = _host(disk_path)
    containers = _container_stats(docker_url, host.get("mem_total_mb")) if docker_url else []
    return {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "host": host,
            "containers": containers}


# ---------------- reading it (pure) ----------------

def mem_used_pct(host: dict) -> float | None:
    if not host.get("mem_total_mb"):
        return None
    return round((host["mem_total_mb"] - host["mem_available_mb"]) / host["mem_total_mb"] * 100, 1)


def _gb(mb: float) -> str:
    return f"{mb / 1024:.1f} GB"


def format_snapshot(snap: dict) -> str:
    h = snap["host"]
    lines = [f"Resources at {snap['at']}"]
    if h.get("mem_total_mb"):
        used = h["mem_total_mb"] - h["mem_available_mb"]
        lines.append(f"host memory: {_gb(used)} used of {_gb(h['mem_total_mb'])} ({mem_used_pct(h)}%), "
                     f"{_gb(h['mem_available_mb'])} available, swap used {_gb(h.get('swap_used_mb', 0))}")
    if "load1" in h:
        lines.append(f"host CPU: load {h['load1']} / {h['load5']} / {h['load15']} (1/5/15 min) on {h['cpus']} cores")
    if "disk_used_pct" in h:
        lines.append(f"disk (lake volume): {h['disk_used_pct']}% used, {h['disk_free_gb']} GB free")
    if snap["containers"]:
        lines.append("containers by memory (running only):")
        for c in snap["containers"]:
            limit = f" of {_gb(c['limit_mb'])} limit ({c['mem_mb'] / c['limit_mb'] * 100:.0f}%)" if c["limit_mb"] else " (no limit)"
            flag = " | AT MEMORY LIMIT" if c["limit_mb"] and c["mem_mb"] >= c["limit_mb"] * AT_LIMIT_PCT / 100 else ""
            lines.append(f"  {c['name']}: mem {_gb(c['mem_mb'])}{limit} | cpu {c['cpu_pct']}% | pids {c['pids']}{flag}")
    else:
        lines.append("containers: per-container stats unavailable (DOCKER_API_URL not set)")
    return "\n".join(lines)


def format_history(rows: list[dict], container: str | None = None) -> str:
    if not rows:
        return "No resource history recorded yet (the health monitor records one snapshot per pass)."
    lines = []
    header = "time (UTC)       | host mem % | load1 | disk % | " + (
        f"{container} mem" if container else "top 3 containers by memory")
    lines.append(header)
    for r in rows:
        h, cs = r["host"], r["containers"]
        if container:
            c = next((c for c in cs if c["name"] == container), None)
            right = (f"{_gb(c['mem_mb'])}, cpu {c['cpu_pct']}%" if c else "NOT RUNNING")
        else:
            right = ", ".join(f"{c['name']} {_gb(c['mem_mb'])}" for c in cs[:3])
        lines.append(f"{r['at'][:16].replace('T', ' ')} | {mem_used_pct(h)!s:>10} | {h.get('load1', '-')!s:>5} | "
                     f"{h.get('disk_used_pct', '-')!s:>6} | {right}")
    peak = max(rows, key=lambda r: mem_used_pct(r["host"]) or 0)
    lines.append(f"({len(rows)} snapshots; peak host memory {mem_used_pct(peak['host'])}% at {peak['at'][:16]})")
    return "\n".join(lines)


def checks(snap: dict) -> list[dict]:
    """Deterministic resource checks: {"check", "ok", "detail"} like the other health checks."""
    h, out = snap["host"], []
    pct = mem_used_pct(h)
    if pct is not None:
        out.append({"check": f"host memory below {MEM_HIGH_PCT}%", "ok": pct < MEM_HIGH_PCT,
                    "detail": f"{pct}% used, {_gb(h['mem_available_mb'])} available"})
    if "load1" in h:
        ok = h["load1"] < h["cpus"] * LOAD_PER_CPU_HIGH
        out.append({"check": "host CPU not saturated", "ok": ok,
                    "detail": f"load {h['load1']} on {h['cpus']} cores"})
    if "disk_used_pct" in h:
        out.append({"check": f"disk below {DISK_HIGH_PCT}%", "ok": h["disk_used_pct"] < DISK_HIGH_PCT,
                    "detail": f"{h['disk_used_pct']}% used, {h['disk_free_gb']} GB free"})
    near = [c for c in snap["containers"] if c["limit_mb"] and c["mem_mb"] >= c["limit_mb"] * AT_LIMIT_PCT / 100]
    if snap["containers"]:
        out.append({"check": "no container at its memory limit", "ok": not near,
                    "detail": ", ".join(f"{c['name']} {c['mem_mb']}/{c['limit_mb']} MB" for c in near)
                    or f"{len(snap['containers'])} containers below their limits"})
    return out


# ---------------- history file ----------------

def history_path() -> Path | None:
    value = os.environ.get("RESOURCE_HISTORY")
    return Path(value) if value else None


def append_history(snap: dict, path: Path) -> None:
    # Compact: drop per-container pids; keep the last HISTORY_DAYS days
    slim = {"at": snap["at"], "host": snap["host"],
            "containers": [{k: c[k] for k in ("name", "mem_mb", "limit_mb", "cpu_pct")} for c in snap["containers"]]}
    rows = read_history(path, hours=HISTORY_DAYS * 24) + [slim]
    tmp = path.with_suffix(".tmp")
    tmp.write_text("".join(json.dumps(r) + "\n" for r in rows))
    tmp.replace(path)                      # atomic: a reader never sees half a file


def read_history(path: Path, hours: int, now: datetime | None = None) -> list[dict]:
    if not path.exists():
        return []
    since = ((now or datetime.now(timezone.utc)) - timedelta(hours=hours)).isoformat()
    rows = []
    for line in path.read_text().splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("at", "") >= since:
            rows.append(row)
    return rows
