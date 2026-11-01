"""A runaway backtest in momentum-scanner (another app on the VM) has pinned all 4 cores
since 11:40. Nothing crashed, every container is running, but weather-stream's
micro-batches now take ~7 minutes instead of 1, so weather lands in the lake ~2 h late.

Right answer: resource_exhaustion (CPU) caused by momentum-scanner, not an upstream NOAA
problem. No automated action: restarting the pipeline doesn't help, and momentum-scanner
isn't a pipeline container. A human stops or throttles it; the stream then catches up."""
from datetime import datetime, timedelta, timezone

from evals.scenario import Scenario
from evals.world import BASE_STATS, healthy_world, host_for

NOW = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)
START = datetime(2026, 10, 5, 11, 40, tzinfo=timezone.utc)
HOG = "momentum-scanner"


def _history() -> list[dict]:
    rows = []
    for i in range(48, 0, -1):
        t = NOW - timedelta(minutes=30 * i)
        busy = t >= START
        stats = [{"name": n, "mem_mb": m, "limit_mb": lim,
                  "cpu_pct": (c * 0.4 if busy and n.endswith("-stream") else c)}
                 for n, (m, lim, c) in BASE_STATS.items()]
        stats.append({"name": HOG, "mem_mb": 900, "limit_mb": None, "cpu_pct": 384.0 if busy else 2.0})
        stats.sort(key=lambda c: -c["mem_mb"])
        rows.append({"at": t.isoformat(timespec="seconds"), "host": host_for(stats, 7.9 if busy else 0.9),
                     "containers": stats})
    return rows


def build() -> Scenario:
    world = healthy_world(NOW)
    world.extra_containers[HOG] = {"status": "Up 6 days"}
    world.container_stats[HOG] = {"mem_mb": 910, "limit_mb": None, "cpu_pct": 386.5, "pids": 18}
    world.container_stats["weather-stream"] = {"cpu_pct": 9.4}
    world.host_load1 = 8.1
    world.resource_history = _history()

    world.lake_lag_minutes["weather"] = 132
    log = ["2026-10-05T11:20:02 INFO MicroBatchExecution: Streaming query made progress: batchId=9601"]
    t = START + timedelta(minutes=5)
    while t < NOW:
        log.append(f"{t:%Y-%m-%dT%H:%M:%S} WARN ProcessingTimeExecutor: Current batch is falling behind. "
                   "The trigger interval is 60000 milliseconds, but spent 418211 milliseconds")
        t += timedelta(minutes=7)
    world.container_logs["weather-stream"] = "\n".join(log)
    return Scenario(
        id="cpu_saturation",
        question="Weather on the dashboard is about two hours behind, but nothing has crashed. Why?",
        world=world,
        expected_category="resource_exhaustion",
        must_mention=[HOG, "cpu"],
        must_not_mention=["noaa"],
        runbook_covered=True,          # runbook_resource_exhaustion.md
        fix_options=[[]],
        notes="Load 8.1 on 4 cores since 11:40, momentum-scanner at ~385% CPU. Restarting the stream won't help.",
    )
