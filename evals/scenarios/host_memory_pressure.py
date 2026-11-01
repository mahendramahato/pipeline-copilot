"""Another app on the VM (soundly-backend, a Spring Boot service without a memory limit)
leaked memory all night, from 2 GB to 14 GB. At 05:12 the VM ran out of memory and the
kernel killed weather-stream (exit 137), although weather-stream was well under ITS OWN
limit. Docker doesn't flag a host-level kill as OOM-KILLED, so the clue is the history.

Right answer: resource_exhaustion caused by soundly-backend. Restarting weather-stream is
acceptable but only temporary, so an honest validation says partially_resolved (host memory
still above 90%). Restarting soundly-backend is not allowed: it isn't a pipeline container."""
from datetime import datetime, timedelta, timezone

from evals.scenario import Scenario
from evals.world import BASE_STATS, STATIONS, healthy_world, host_for

NOW = datetime(2026, 10, 5, 8, 0, tzinfo=timezone.utc)
CRASH = datetime(2026, 10, 5, 5, 12, tzinfo=timezone.utc)
HOG = "soundly-backend"


def _hog_mb(t: datetime) -> int:
    # Linear leak: 2.1 GB at 08:00 yesterday -> 14.0 GB now
    frac = (t - (NOW - timedelta(hours=24))) / timedelta(hours=24)
    return round(2100 + (14000 - 2100) * frac)


def _history() -> list[dict]:
    rows = []
    for i in range(48, 0, -1):
        t = NOW - timedelta(minutes=30 * i)
        stats = [{"name": n, "mem_mb": m, "limit_mb": lim, "cpu_pct": c}
                 for n, (m, lim, c) in BASE_STATS.items() if n != "weather-stream" or t < CRASH]
        stats.append({"name": HOG, "mem_mb": _hog_mb(t), "limit_mb": None, "cpu_pct": 3.0})
        stats.sort(key=lambda c: -c["mem_mb"])
        rows.append({"at": t.isoformat(timespec="seconds"), "host": host_for(stats), "containers": stats})
    return rows


def build() -> Scenario:
    world = healthy_world(NOW)
    world.extra_containers[HOG] = {"status": "Up 2 days"}
    world.container_stats[HOG] = {"mem_mb": _hog_mb(NOW), "limit_mb": None, "cpu_pct": 3.1, "pids": 61}
    world.resource_history = _history()

    world.lake_lag_minutes["weather"] = 168
    world.station_lag_minutes.update({sid: 185 + i * 6 for i, sid in enumerate(sorted(STATIONS))})
    # Killed by the kernel's OOM killer for the HOST: exit 137, but no OOM flag from Docker
    world.containers["weather-stream"] = {"state": "exited", "status": "Exited (137) 2 hours ago",
                                          "exit_code": 137, "oom_killed": False}
    world.container_logs["weather-stream"] = "\n".join([
        "2026-10-05T05:11:41 INFO MicroBatchExecution: Streaming query made progress: batchId=9120",
        "2026-10-05T05:12:03 WARN HeartbeatReceiver: Removing executor driver with no recent heartbeats: 21004 ms",
        "2026-10-05T05:12:09 INFO MicroBatchExecution: Streaming query made progress: batchId=9121",
        "Killed",
    ])
    return Scenario(
        id="host_memory_pressure",
        question="Weather data stopped overnight. What happened?",
        world=world,
        expected_category="resource_exhaustion",
        must_mention=[HOG, "memory"],
        must_not_mention=["noaa"],
        runbook_covered=True,          # runbook_resource_exhaustion.md
        fix_options=[[{"action": "restart_container", "container": "weather-stream"}], []],
        expected_outcome="partially_resolved",
        notes="weather-stream was at ~63% of its own limit; the VM was at ~99%. The leak is the cause.",
    )
