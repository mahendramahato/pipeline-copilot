"""Resource parsing, thresholds and history: pure functions, no Docker needed."""
import json
from datetime import datetime, timedelta, timezone

from pipeline_copilot import resources as r

MEMINFO = """MemTotal:       24576000 kB
MemFree:          512000 kB
MemAvailable:    1228800 kB
SwapTotal:       2097152 kB
SwapFree:        1048576 kB
"""

GB = 2**30


def _stats(usage, inactive, limit, cpu_now, cpu_before, sys_now, sys_before, cpus=4):
    return {
        "memory_stats": {"usage": usage, "limit": limit, "stats": {"inactive_file": inactive}},
        "cpu_stats": {"cpu_usage": {"total_usage": cpu_now}, "system_cpu_usage": sys_now, "online_cpus": cpus},
        "precpu_stats": {"cpu_usage": {"total_usage": cpu_before}, "system_cpu_usage": sys_before},
        "pids_stats": {"current": 42},
    }


def test_parse_meminfo():
    m = r.parse_meminfo(MEMINFO)
    assert m == {"mem_total_mb": 24000, "mem_available_mb": 1200, "swap_used_mb": 1024}


def test_docker_stats_memory_excludes_page_cache_and_limit():
    s = r.parse_docker_stats("weather-stream", _stats(3 * GB, GB, 4 * GB, 0, 0, 0, 0), host_total_mb=24000)
    assert s["mem_mb"] == 2048 and s["limit_mb"] == 4096


def test_docker_stats_no_limit_reported_as_none():
    # Docker reports the host's memory as the "limit" when the container has none
    s = r.parse_docker_stats("soundly-backend", _stats(GB, 0, 24000 * 2**20, 0, 0, 0, 0), host_total_mb=24000)
    assert s["limit_mb"] is None


def test_docker_stats_cpu_percent():
    # Used 2/4 of the system delta's worth of one interval across 4 cpus = 200%
    s = r.parse_docker_stats("x", _stats(0, 0, GB, 2_000, 1_000, 4_000, 2_000, cpus=4), host_total_mb=24000)
    assert s["cpu_pct"] == 200.0
    assert s["pids"] == 42


def _snap(available_mb, load1=0.5, disk=40.0, containers=()):
    return {"at": "2026-10-05T08:00:00+00:00",
            "host": {"cpus": 4, "mem_total_mb": 24000, "mem_available_mb": available_mb, "swap_used_mb": 0,
                     "load1": load1, "load5": load1, "load15": load1, "disk_used_pct": disk, "disk_free_gb": 50.0},
            "containers": list(containers)}


def test_checks_healthy():
    snap = _snap(12000, containers=[{"name": "a", "mem_mb": 100, "limit_mb": 1000, "cpu_pct": 1, "pids": 1}])
    assert all(c["ok"] for c in r.checks(snap))


def test_checks_flag_memory_cpu_disk_and_limit():
    snap = _snap(1000, load1=8.0, disk=91.0,
                 containers=[{"name": "weather-stream", "mem_mb": 3900, "limit_mb": 4096, "cpu_pct": 50, "pids": 1}])
    failing = {c["check"] for c in r.checks(snap) if not c["ok"]}
    assert failing == {"host memory below 90%", "host CPU not saturated", "disk below 85%",
                       "no container at its memory limit"}
    assert "AT MEMORY LIMIT" in r.format_snapshot(snap)


def test_history_append_trims_and_reads(tmp_path):
    path = tmp_path / "resources.jsonl"
    now = datetime.now(timezone.utc)
    old = _snap(9000)
    old["at"] = (now - timedelta(days=8)).isoformat(timespec="seconds")
    path.write_text(json.dumps(old) + "\n")
    new = _snap(5000, containers=[{"name": "a", "mem_mb": 1, "limit_mb": None, "cpu_pct": 0, "pids": 3}])
    new["at"] = now.isoformat(timespec="seconds")
    r.append_history(new, path)
    rows = r.read_history(path, hours=24 * 30)
    assert len(rows) == 1                              # the 8-day-old row was dropped
    assert "pids" not in rows[0]["containers"][0]      # stored compactly


def test_format_history_peak_and_one_container():
    a, b = _snap(12000), _snap(500, containers=[{"name": "x", "mem_mb": 9000, "limit_mb": None, "cpu_pct": 1}])
    b["at"] = "2026-10-05T09:00:00+00:00"
    text = r.format_history([a, b])
    assert "peak host memory 97.9% at 2026-10-05T09:00" in text
    assert "NOT RUNNING" in r.format_history([a], container="x")
