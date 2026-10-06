"""A simulated pipeline "world" for evals: the data the fake tools will serve.

healthy_world() builds a week of a WORKING pipeline (green runs, raw and curated
tables that agree). Each scenario then breaks one thing in it. Randomness is
seeded, so every run builds identical data.
"""
import hashlib
import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone

DAG_ID = "daily_lake_maintenance"
TASKS = ["check_freshness", "target_date", "sync_to_s3", "curate_day"]
STATIONS = {  # station_id: (lat, lon, typical temperature °C)
    "KLAX": (33.93, -118.40, 22.0),
    "KBOI": (43.56, -116.22, 15.0),
    "KDEN": (39.86, -104.67, 12.0),
    "KSEA": (47.45, -122.31, 13.0),
    "KORD": (41.98, -87.90, 14.0),
}

# Column types per table, mirroring the real Glue tables (what get_table_schema reports).
_WEATHER = {
    "station_id": "string", "observed_at": "timestamp", "lat": "double", "lon": "double",
    "temperature_c": "double", "humidity_pct": "double", "wind_speed_kmh": "double",
    "description": "string", "ingested_at": "timestamp", "baseline_avg": "double",
    "baseline_std": "double", "baseline_count": "bigint", "baseline_hours": "int",
    "z_score": "double", "is_anomaly": "boolean",
}
_SEISMIC = {
    "event_id": "string", "event_time": "timestamp", "updated_at": "timestamp", "lat": "double",
    "lon": "double", "magnitude": "double", "place": "string", "depth_km": "double",
    "ingested_at": "timestamp", "is_significant": "boolean",
}
SCHEMAS = {
    "raw_weather": _WEATHER, "curated_weather": _WEATHER,
    "raw_seismic": _SEISMIC, "curated_seismic": _SEISMIC,
    "curated_weather_daily": {
        "station_id": "string", "readings": "bigint", "min_temp_c": "double", "max_temp_c": "double",
        "avg_temp_c": "double", "scored_readings": "bigint", "anomalies": "bigint",
    },
}


@dataclass
class World:
    now: datetime
    dag_runs: list[dict] = field(default_factory=list)                    # newest first
    task_instances: dict[str, list[dict]] = field(default_factory=dict)   # run_id -> tasks
    task_logs: dict[tuple[str, str], str] = field(default_factory=dict)   # (run_id, task_id) -> log text
    tables: dict[str, list[dict]] = field(default_factory=dict)           # table -> rows (each has "date")
    registered_partitions: dict[str, list[str]] = field(default_factory=dict)
    table_versions: dict[str, list[dict]] = field(default_factory=dict)   # table -> [{version, updated, columns}]

    # --- Rebuild curated tables from raw, up to yesterday (what the nightly Glue job does) ---
    # Scenarios break RAW data, then call this so the damage flows downstream realistically.
    def recurate(self) -> None:
        last = (self.now.date() - timedelta(days=1)).isoformat()
        raw_w = [r for r in self.tables["raw_weather"] if r["date"] <= last]
        raw_s = [r for r in self.tables["raw_seismic"] if r["date"] <= last]
        self.tables["curated_weather"] = _latest(raw_w, ("station_id", "observed_at"), "ingested_at")
        self.tables["curated_seismic"] = _latest(raw_s, ("event_id",), "updated_at")
        self.tables["curated_weather_daily"] = _daily(self.tables["curated_weather"])


# --- Small helpers ---
def _ts(d: date, minutes: int) -> datetime:
    return datetime.combine(d, time()) + timedelta(minutes=minutes)    # naive UTC, like Athena timestamps


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _log(dt: datetime, level: str, msg: str) -> str:
    return f"{dt:%H:%M:%S} {level.upper():8} {msg}"                    # same format as the real log tool


def _latest(rows: list[dict], keys: tuple, order_col: str) -> list[dict]:
    best: dict = {}
    for r in rows:
        k = tuple(r[c] for c in keys)
        if k not in best or r[order_col] > best[k][order_col]:
            best[k] = r
    return [dict(r) for r in best.values()]


def _daily(curated_weather: list[dict]) -> list[dict]:
    groups: dict = {}
    for r in curated_weather:
        groups.setdefault((r["date"], r["station_id"]), []).append(r)
    out = []
    for (d, sid), rs in sorted(groups.items()):
        temps = [r["temperature_c"] for r in rs if r["temperature_c"] is not None]
        out.append({
            "station_id": sid, "readings": len(rs),
            "min_temp_c": min(temps) if temps else None, "max_temp_c": max(temps) if temps else None,
            "avg_temp_c": round(sum(temps) / len(temps), 1) if temps else None,
            "scored_readings": sum(r["z_score"] is not None for r in rs),
            "anomalies": sum(bool(r["is_anomaly"]) for r in rs), "date": d,
        })
    return out


# --- Raw data generators ---
def _weather_rows(d: date, rng: random.Random, minutes_max: int = 24 * 60) -> list[dict]:
    rows = []
    for sid, (lat, lon, base) in STATIONS.items():
        for m in range(0, minutes_max, 15):                            # one reading per 15 min
            obs = _ts(d, m)
            temp = round(base + rng.uniform(-5, 5), 1)
            rows.append({
                "station_id": sid, "observed_at": obs, "lat": lat, "lon": lon,
                "temperature_c": temp, "humidity_pct": round(rng.uniform(20, 90), 1),
                "wind_speed_kmh": round(rng.uniform(0, 30), 1), "description": "clear",
                "ingested_at": obs + timedelta(minutes=1), "baseline_avg": base, "baseline_std": 3.0,
                "baseline_count": 96, "baseline_hours": 24, "z_score": round((temp - base) / 3.0, 2),
                "is_anomaly": False, "date": d.isoformat(),
            })
    return rows


def _seismic_rows(d: date, rng: random.Random, n_events: int = 120, minutes_max: int = 24 * 60 - 30) -> list[dict]:
    rows = []
    for i in range(n_events):
        t = _ts(d, rng.randrange(minutes_max))
        mag = round(rng.uniform(0.5, 5.5), 1)
        event = {
            "event_id": f"us{d:%Y%m%d}{i:04d}", "event_time": t, "magnitude": mag,
            "lat": round(rng.uniform(-60, 60), 2), "lon": round(rng.uniform(-180, 180), 2),
            "place": f"{rng.randint(5, 80)} km from a fault line", "depth_km": round(rng.uniform(1, 70), 1),
            "is_significant": mag >= 4.5, "date": d.isoformat(),
        }
        # USGS revises events: raw keeps every revision, curated keeps the latest
        for rev in (1, 2):
            rows.append({**event, "updated_at": t + timedelta(minutes=10 * rev),
                         "ingested_at": t + timedelta(minutes=10 * rev + 1)})
    return rows


# --- The healthy baseline ---
def healthy_world(now: datetime, days: int = 6, seed: int = 7) -> World:
    rng = random.Random(seed)
    today = now.date()
    full_days = [today - timedelta(days=i) for i in range(days, 0, -1)]    # oldest .. yesterday
    world = World(now=now)

    # Raw: full days, plus today's first 30 minutes (what the 00:30 sync uploaded)
    raw_w, raw_s = [], []
    for d in full_days:
        raw_w += _weather_rows(d, rng)
        raw_s += _seismic_rows(d, rng)
    raw_w += _weather_rows(today, rng, minutes_max=30)
    raw_s += _seismic_rows(today, rng, n_events=2, minutes_max=30)
    world.tables = {"raw_weather": raw_w, "raw_seismic": raw_s}
    world.recurate()

    # Airflow: the run on day X curates day X-1, so runs go from full_days[0]+1 to today
    for run_day in [d + timedelta(days=1) for d in full_days]:
        run_id = f"scheduled__{run_day}T00:30:00+00:00"
        start = datetime.combine(run_day, time(0, 30), tzinfo=timezone.utc)
        target = (run_day - timedelta(days=1)).isoformat()
        glue_run = "jr_" + hashlib.sha256(run_id.encode()).hexdigest()
        world.dag_runs.insert(0, {
            "dag_run_id": run_id, "state": "success", "queued_at": _iso(start),
            "start_date": _iso(start + timedelta(seconds=1)), "end_date": _iso(start + timedelta(minutes=2)),
        })
        world.task_instances[run_id] = [
            {"task_id": "check_freshness", "state": "success", "try_number": 1, "duration": 1.1},
            {"task_id": "target_date", "state": "success", "try_number": 1, "duration": 0.6},
            {"task_id": "sync_to_s3", "state": "success", "try_number": 1, "duration": 16.4},
            {"task_id": "curate_day", "state": "success", "try_number": 1, "duration": 84.9},
        ]
        t = start + timedelta(seconds=5)
        world.task_logs[(run_id, "check_freshness")] = "\n".join([
            _log(t, "info", "weather: newest file is 0.1h old"),
            _log(t, "info", "seismic: newest file is 0.1h old"),
            _log(t, "info", "Done. Returned value was: None"),
        ])
        world.task_logs[(run_id, "target_date")] = _log(t, "info", f"Done. Returned value was: {target}")
        world.task_logs[(run_id, "sync_to_s3")] = "\n".join([
            _log(t, "info", "syncing weather..."), _log(t, "info", "syncing seismic..."),
            _log(t, "info", "done"), _log(t, "info", "Command exited with return code 0"),
        ])
        world.task_logs[(run_id, "curate_day")] = "\n".join([
            _log(t, "info", f"Starting AWS Glue Job: curate-daily with --DATE {target}"),
            _log(t + timedelta(seconds=84), "info", f"AWS Glue Job: curate-daily status: SUCCEEDED. Run Id: {glue_run}"),
        ])

    # Glue catalog: in this simulated (fixed) pipeline, raw partitions ARE registered
    for t_name in ("raw_weather", "raw_seismic"):
        world.registered_partitions[t_name] = sorted({r["date"] for r in world.tables[t_name]})
    for t_name, cols in SCHEMAS.items():
        world.table_versions[t_name] = [{"version": 0, "updated": "2026-09-27T00:00:00Z", "columns": dict(cols)}]
    return world
