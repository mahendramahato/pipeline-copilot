"""Live, read-only views of the pipeline host, for the Ops MCP server.

- The local lake the Spark jobs write (Parquet), read with DuckDB: minutes-fresh,
  unlike Athena's raw tables, which only update at the nightly sync.
- Container status and logs, through a Docker API proxy that only allows reads.
- The dashboard's health: its internal API and the public HTTPS site.

Settings come from the environment (no secrets): LAKE_DIR, DOCKER_API_URL,
DASHBOARD_API_URL, DASHBOARD_PUBLIC_URL, EXPECTED_CONTAINERS.
"""
import os
import re
import struct
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import httpx

from pipeline_copilot.sql_guard import check_query

LAKE_TABLES = {"lake_weather": "weather", "lake_seismic": "seismic"}
STALE_FILE_MINUTES = 90        # no new lake file for this long = a feed has probably stopped
STALE_STATION_MINUTES = 180    # NOAA stations report roughly hourly; 3 h behind = that station's feed is stuck
MAX_LINE_CHARS = 300
MAX_LOG_CHARS = 8000
CONTAINER_NAME = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$")
DEFAULT_EXPECTED = "kafka,producer,weather-stream,seismic-stream,airflow,airflow-postgres,api,web"


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------- the live local lake ----------------

def lake_dir() -> Path | None:
    value = os.environ.get("LAKE_DIR")
    return Path(value) if value and Path(value).is_dir() else None


def _newest_file(folder: Path) -> tuple[float | None, str | None]:
    # Only committed files under date=*/ (Spark stages in-progress files elsewhere)
    newest, partition = None, None
    for path in folder.glob("date=*/*.parquet"):
        mtime = path.stat().st_mtime
        if newest is None or mtime > newest:
            newest, partition = mtime, path.parent.name
    return ((time.time() - newest) / 60 if newest else None), partition


def lake_connection(root: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    for view, folder in LAKE_TABLES.items():
        pattern = (root / folder / "date=*" / "*.parquet").as_posix()
        con.execute(
            f"CREATE VIEW {view} AS SELECT * FROM read_parquet('{pattern}', "
            "hive_partitioning = true, union_by_name = true)"
        )
    return con


def lake_freshness(root: Path) -> dict:
    now = _now()
    since = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    result = {"now": now.isoformat(timespec="seconds"), "datasets": {}, "stations": [], "seismic": None}
    for folder in LAKE_TABLES.values():
        age, partition = _newest_file(root / folder)
        result["datasets"][folder] = {
            "newest_file_minutes": None if age is None else round(age, 1),
            "partition": partition,
            "stale": age is None or age > STALE_FILE_MINUTES,
        }
    con = lake_connection(root)
    # epoch() turns timestamps into UTC seconds, whatever time-zone type Parquet stored
    try:
        for station, latest, readings in con.execute(
            "SELECT station_id, epoch(max(observed_at)), count(*) FROM lake_weather "
            "WHERE CAST(date AS VARCHAR) >= ? GROUP BY station_id ORDER BY station_id", [since]
        ).fetchall():
            behind = (now.timestamp() - latest) / 60
            result["stations"].append({"station": station, "minutes_behind": round(behind),
                                       "readings_since_yesterday": readings,
                                       "stale": behind > STALE_STATION_MINUTES})
    except duckdb.Error as e:
        result["stations_error"] = str(e).splitlines()[0]
    try:
        latest, events = con.execute(
            "SELECT epoch(max(ingested_at)), count(DISTINCT event_id) FROM lake_seismic "
            "WHERE CAST(date AS VARCHAR) = ?", [now.strftime("%Y-%m-%d")]
        ).fetchone()
        result["seismic"] = {"events_today": events,
                             "last_ingest_minutes": None if latest is None else round((now.timestamp() - latest) / 60)}
    except duckdb.Error as e:
        result["seismic_error"] = str(e).splitlines()[0]
    return result


def format_freshness(f: dict) -> str:
    lines = [f"now (UTC): {f['now']}", "newest file written by each streaming job:"]
    for name, d in f["datasets"].items():
        age = "no files" if d["newest_file_minutes"] is None else f"{d['newest_file_minutes']} min ago ({d['partition']})"
        lines.append(f"  {name}: {age}{'  <-- STALE' if d['stale'] else ''}")
    if f.get("stations"):
        lines.append("weather stations (latest observation, minutes behind now):")
        lines += [f"  {s['station']}: {s['minutes_behind']} min, {s['readings_since_yesterday']} readings"
                  f"{'  <-- STALE' if s['stale'] else ''}" for s in f["stations"]]
    if f.get("stations_error"):
        lines.append(f"weather stations: unavailable ({f['stations_error']})")
    if f.get("seismic"):
        s = f["seismic"]
        lines.append(f"seismic today: {s['events_today']} distinct events, last ingest {s['last_ingest_minutes']} min ago")
    if f.get("seismic_error"):
        lines.append(f"seismic: unavailable ({f['seismic_error']})")
    return "\n".join(lines)


def query_lake(root: Path, sql: str, max_rows: int = 100) -> str:
    # The same SQL guardrail as Athena, with DuckDB's dialect and the lake views as the allow-list
    safe_sql = check_query(sql, "lake", set(LAKE_TABLES), dialect="duckdb")
    try:
        cur = lake_connection(root).execute(safe_sql)
    except duckdb.Error as e:
        return f"Query FAILED: {str(e).splitlines()[0]}\nSQL that ran: {safe_sql}"
    columns = [d[0] for d in cur.description]
    rows = cur.fetchmany(max_rows + 1)
    lines = [" | ".join(columns)]
    lines += [" | ".join("NULL" if v is None else str(v) for v in row) for row in rows[:max_rows]]
    note = "; TRUNCATED, more rows exist" if len(rows) > max_rows else ""
    lines.append(f"({min(len(rows), max_rows)} rows{note}; live local lake)")
    return "\n".join(lines)


# ---------------- containers (Docker API through a read-only proxy) ----------------

def docker_url() -> str | None:
    return os.environ.get("DOCKER_API_URL") or None


def list_containers(url: str) -> list[dict]:
    with httpx.Client(base_url=url, timeout=10) as client:
        items = client.get("/containers/json", params={"all": "true"})
        items.raise_for_status()
        out = []
        for item in items.json():
            info = client.get(f"/containers/{item['Id']}/json").json()
            state = info.get("State", {})
            out.append({
                "name": item["Names"][0].lstrip("/"),
                "state": state.get("Status"),
                "status": item.get("Status"),
                "restarts": info.get("RestartCount", 0),
                "exit_code": state.get("ExitCode"),
                "oom_killed": state.get("OOMKilled", False),
            })
    return sorted(out, key=lambda c: c["name"])


def format_containers(containers: list[dict]) -> str:
    return "\n".join(
        f"{c['name']}: {c['state']} ({c['status']}) | restarts={c['restarts']} | exit_code={c['exit_code']}"
        + (" | OOM-KILLED" if c["oom_killed"] else "")
        for c in containers
    ) or "No containers found."


def demux_docker_logs(raw: bytes) -> str:
    """Docker multiplexes stdout/stderr: each frame is an 8-byte header (stream type,
    3 zero bytes, payload length) then the payload. Containers with a TTY send plain text."""
    if len(raw) < 8 or raw[0] not in (0, 1, 2) or raw[1:4] != b"\x00\x00\x00":
        return raw.decode("utf-8", "replace")
    chunks, i = [], 0
    while i + 8 <= len(raw):
        size = struct.unpack(">I", raw[i + 4:i + 8])[0]
        chunks.append(raw[i + 8:i + 8 + size])
        i += 8 + size
    return b"".join(chunks).decode("utf-8", "replace")


def container_logs(url: str, name: str, tail: int = 100) -> str:
    if not CONTAINER_NAME.match(name):
        return f"Invalid container name: {name!r}"
    with httpx.Client(base_url=url, timeout=15) as client:
        resp = client.get(f"/containers/{name}/logs",
                          params={"stdout": "1", "stderr": "1", "tail": str(tail), "timestamps": "1"})
    if resp.status_code == 404:
        return f"No container named {name!r}. Use list_containers to see the names."
    resp.raise_for_status()
    lines = []
    for line in demux_docker_logs(resp.content).splitlines():
        ts, _, msg = line.partition(" ")
        lines.append(f"{ts[:19]} {msg}"[:MAX_LINE_CHARS])
    text = "\n".join(lines) or "(no log output)"
    # Keep the end: that's where crashes and the latest state are
    return text if len(text) <= MAX_LOG_CHARS else "(cut to the last part)\n" + text[-MAX_LOG_CHARS:]


# ---------------- the dashboard ----------------

def dashboard_health() -> dict:
    api = os.environ.get("DASHBOARD_API_URL")
    public = os.environ.get("DASHBOARD_PUBLIC_URL")
    targets = {}
    if api:
        targets["api_health"] = f"{api}/api/health"
        targets["api_stations"] = f"{api}/api/stations"
    if public:
        targets["public_site"] = public
    results = {}
    with httpx.Client(timeout=10, follow_redirects=True) as client:
        for label, url in targets.items():
            started = time.monotonic()
            try:
                resp = client.get(url)
            except httpx.HTTPError as e:
                results[label] = {"ok": False, "error": type(e).__name__}
                continue
            entry = {"ok": resp.status_code == 200, "status": resp.status_code,
                     "ms": round((time.monotonic() - started) * 1000)}
            if label == "api_stations" and resp.status_code == 200:
                try:
                    stations = resp.json()
                    stations = stations.get("stations", stations) if isinstance(stations, dict) else stations
                    entry["stations"] = len(stations)
                    entry["by_status"] = dict(Counter(s.get("status", "?") for s in stations if isinstance(s, dict)))
                except ValueError:
                    pass
            results[label] = entry
    return results


def format_dashboard(results: dict) -> str:
    if not results:
        return "Dashboard checks are not configured (DASHBOARD_API_URL / DASHBOARD_PUBLIC_URL)."
    return "\n".join(
        f"{label}: {'OK' if r['ok'] else 'FAILING'} | "
        + (f"error={r['error']}" if "error" in r else f"HTTP {r['status']} in {r['ms']} ms")
        + (f" | {r['stations']} stations {r['by_status']}" if "stations" in r else "")
        for label, r in results.items()
    )


# ---------------- all deterministic checks at once (used by the monitor) ----------------

def health_checks() -> list[dict]:
    checks: list[dict] = []
    root = lake_dir()
    if root:
        f = lake_freshness(root)
        for name, d in f["datasets"].items():
            age = d["newest_file_minutes"]
            checks.append({"check": f"lake {name} receiving data", "ok": not d["stale"],
                           "detail": "no files" if age is None else f"newest file {age} min ago"})
        stale = [s["station"] for s in f.get("stations", []) if s["stale"]]
        if f.get("stations"):
            checks.append({"check": "all weather stations reporting", "ok": not stale,
                           "detail": f"stale: {', '.join(stale)}" if stale else f"{len(f['stations'])} stations current"})
    url = docker_url()
    if url:
        try:
            running = {c["name"]: c for c in list_containers(url)}
            expected = [c.strip() for c in os.environ.get("EXPECTED_CONTAINERS", DEFAULT_EXPECTED).split(",") if c.strip()]
            for name in expected:
                c = running.get(name)
                checks.append({"check": f"container {name} running", "ok": bool(c) and c["state"] == "running",
                               "detail": "missing" if not c else f"{c['state']} ({c['status']}), restarts={c['restarts']}"})
        except httpx.HTTPError as e:
            checks.append({"check": "docker API reachable", "ok": False, "detail": type(e).__name__})
    for label, r in dashboard_health().items():
        checks.append({"check": f"dashboard {label}", "ok": r["ok"],
                       "detail": r.get("error") or f"HTTP {r['status']} in {r['ms']} ms"})
    return checks
