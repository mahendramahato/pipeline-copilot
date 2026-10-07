"""Retrieval tests: does the right document come back for realistic questions?

Builds a fresh index in a temp folder (independent of your .chroma/), then
checks that the expected document is among the top K results. These are the
questions an on-call engineer, or the agent, would actually ask.
"""
import pytest

from pipeline_copilot.config import KnowledgeSettings, load_knowledge_settings
from pipeline_copilot.knowledge_base import build_index, search

K = 3   # the agent will receive this many chunks per search

CASES = [
    # --- curated data missing (silent failure) ---
    ("curated table has no data for yesterday but the Glue job succeeded", "runbook_curated_data_missing.md"),
    ("Glue job read zero rows even though Athena shows the new partitions", "runbook_curated_data_missing.md"),
    ("DAG is green but curated data stopped updating", "runbook_curated_data_missing.md"),
    # --- streaming / freshness ---
    ("check_freshness failed: weather lake is stale", "runbook_check_freshness_failed.md"),
    ("the seismic streaming job stopped", "runbook_check_freshness_failed.md"),        # hard: 0.27 in the experiment
    # --- schema drift ---
    ("magnitude column is suddenly all NULL", "runbook_schema_drift.md"),
    ("USGS renamed a field in their API", "runbook_schema_drift.md"),
    # --- backfill ---
    ("how do I re-run curation for a past day?", "runbook_backfill.md"),
    # --- Glue job failures ---
    ("ConcurrentRunsExceededException", "runbook_glue_job_failed.md"),
    ("curate_day task failed", "runbook_glue_job_failed.md"),
    # --- how the pipeline normally behaves ---
    ("why is raw data in Athena a day behind?", "pipeline_overview.md"),
    ("how many weather rows per day is normal?", "pipeline_overview.md"),
    # --- technology explainers and architecture ---
    ("what is Apache Kafka and what does it do here?", "tech_kafka.md"),
    ("what happens to messages if the Spark job is down?", "tech_kafka.md"),
    ("how does the weather anomaly detection work?", "tech_spark_streaming.md"),
    ("what is Spark Structured Streaming?", "tech_spark_streaming.md"),
    ("what is Airflow and what does the DAG do?", "tech_airflow.md"),
    ("what is AWS Glue?", "tech_aws_data_lake.md"),
    ("what is partition projection in Athena?", "tech_aws_data_lake.md"),
    ("why use Parquet files?", "tech_aws_data_lake.md"),
    ("how are all the services connected end to end?", "architecture_end_to_end.md"),
    ("where does each service run?", "architecture_end_to_end.md"),
    ("what is MCP and how does this assistant use it?", "tech_pipeline_copilot.md"),
    ("what is RAG?", "tech_pipeline_copilot.md"),
]


# --- One fresh index for the whole test file (building it takes a few seconds) ---
@pytest.fixture(scope="module")
def settings(tmp_path_factory) -> KnowledgeSettings:
    s = KnowledgeSettings(
        knowledge_dir=load_knowledge_settings().knowledge_dir,   # the real docs
        chroma_path=tmp_path_factory.mktemp("chroma"),            # a throwaway index
    )
    build_index(s)
    return s


@pytest.mark.parametrize("query, expected", CASES)
def test_expected_doc_in_top_k(settings, query, expected):
    results = search(query, K, settings)
    sources = [r["source"] for r in results]
    # On failure, show what DID come back: that's what tells you how to fix the docs
    assert expected in sources, (
        f"\n  query:    {query!r}\n  expected: {expected}\n  got:      "
        + "\n            ".join(f"{r['id']} (distance {r['distance']:.3f})" for r in results)
    )

