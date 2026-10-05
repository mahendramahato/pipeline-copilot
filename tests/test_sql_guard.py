"""Tests for the SQL guardrail. No AWS needed: check_query is a pure function.

Every attack we can think of gets a test. If someone later "simplifies" the
guard and breaks a rule, these tests fail before the change ships.
"""
import pytest

from pipeline_copilot.sql_guard import DEFAULT_LIMIT, MAX_LIMIT, UnsafeQueryError, check_query

DB = "weather_seismic"
TABLES = {"raw_weather", "raw_seismic", "curated_weather", "curated_seismic",
          "curated_weather_daily", "alerts"}


def check(sql: str) -> str:
    return check_query(sql, DB, TABLES)


# --- Queries that must be ALLOWED ---

def test_simple_select_gets_default_limit():
    assert f"LIMIT {DEFAULT_LIMIT}" in check("SELECT * FROM raw_seismic")

def test_existing_small_limit_is_kept():
    assert "LIMIT 5" in check("SELECT * FROM raw_weather LIMIT 5")

def test_huge_limit_is_capped():
    assert f"LIMIT {MAX_LIMIT}" in check("SELECT * FROM raw_weather LIMIT 50000")

def test_aggregate_query_allowed():
    check("SELECT date, COUNT(*) FROM curated_seismic GROUP BY date ORDER BY date DESC")

def test_database_qualified_table_allowed():
    check(f"SELECT * FROM {DB}.raw_weather")

def test_cte_allowed():
    check("WITH recent AS (SELECT * FROM raw_weather) SELECT COUNT(*) FROM recent")

def test_union_allowed():
    check("SELECT date FROM raw_weather UNION ALL SELECT date FROM curated_weather")

def test_trailing_semicolon_allowed():
    check("SELECT * FROM raw_seismic;")


# --- Queries that must be BLOCKED ---

@pytest.mark.parametrize("sql", [
    "SELECT 1; DROP TABLE raw_weather",                              # stacked statements
    "DROP TABLE raw_weather",
    "/* looks harmless */ DELETE FROM raw_weather",
    "INSERT INTO raw_weather SELECT * FROM raw_weather",
    "WITH x AS (SELECT 1) INSERT INTO raw_weather SELECT * FROM x",  # starts with WITH
    "CREATE TABLE stolen AS SELECT * FROM raw_weather",
    "ALTER TABLE raw_weather ADD COLUMNS (x int)",
    "MSCK REPAIR TABLE raw_weather",
    "SELECT * FROM other_db.secrets",                                 # other database
    "SELECT * FROM information_schema.tables",                        # catalog snooping
    "SELECT * FROM made_up_table",                                    # unknown table
    "",                                                               # empty
])
def test_unsafe_queries_are_blocked(sql):
    with pytest.raises(UnsafeQueryError):
        check(sql)


def test_error_message_lists_allowed_tables():
    # The agent relies on this message to correct itself
    with pytest.raises(UnsafeQueryError, match="curated_seismic"):
        check("SELECT * FROM curated_quakes")
