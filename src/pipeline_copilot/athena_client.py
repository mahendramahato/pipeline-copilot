"""Thin client for Athena (run queries) and Glue (table metadata).

No safety checks here: the SQL guardrail lives in the MCP server, in front
of this class. This class only knows HOW to talk to AWS.
"""
import time
from dataclasses import dataclass

import boto3

from pipeline_copilot.config import AthenaSettings


class AthenaQueryError(Exception):
    """The query failed, was cancelled, or timed out."""


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list[str | None]]
    truncated: bool          # True if more rows existed than we fetched
    bytes_scanned: int       # what Athena billed for
    query_id: str


class AthenaClient:
    def __init__(self, settings: AthenaSettings, timeout_s: float = 60.0):
        # --- One AWS session, using the read-only agent profile ---
        # profile_name picks the keys from ~/.aws/credentials. Being explicit
        # (instead of relying on the AWS_PROFILE env var) makes it impossible
        # to accidentally run as your admin user.
        session = boto3.Session(profile_name=settings.aws_profile, region_name=settings.aws_region)
        self._athena = session.client("athena")
        self._glue = session.client("glue")
        self._settings = settings
        self._timeout_s = timeout_s

    # --- Run one query and wait for it ---
    def run_query(self, sql: str, max_rows: int = 100) -> QueryResult:
        qid = self._athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self._settings.database},
            # The workgroup enforces the results location AND the 1 GB scan limit
            WorkGroup=self._settings.workgroup,
        )["QueryExecutionId"]

        execution = self._wait(qid)

        # +1 because the first row of a SELECT result is the column headers
        page = self._athena.get_query_results(QueryExecutionId=qid, MaxResults=max_rows + 1)
        columns = [c["Name"] for c in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        all_rows = [[cell.get("VarCharValue") for cell in r["Data"]] for r in page["ResultSet"]["Rows"]]

        return QueryResult(
            columns=columns,
            rows=all_rows[1:],                      # drop the header row
            truncated="NextToken" in page,          # more pages exist = we didn't get everything
            bytes_scanned=execution["Statistics"].get("DataScannedInBytes", 0),
            query_id=qid,
        )

    # --- Poll until the query finishes ---
    # Backoff: start polling fast (most queries take 1-3s), slow down after.
    # On timeout we CANCEL the query in AWS. Just walking away would leave it
    # running, and still billing.
    def _wait(self, qid: str) -> dict:
        deadline = time.monotonic() + self._timeout_s
        delay = 0.5
        while True:
            execution = self._athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            state = execution["Status"]["State"]
            if state == "SUCCEEDED":
                return execution
            if state in ("FAILED", "CANCELLED"):
                reason = execution["Status"].get("StateChangeReason", "no reason given")
                raise AthenaQueryError(f"Query {state}: {reason}")
            if time.monotonic() > deadline:
                self._athena.stop_query_execution(QueryExecutionId=qid)
                raise AthenaQueryError(f"Query timed out after {self._timeout_s:.0f}s and was cancelled")
            time.sleep(delay)
            delay = min(delay * 1.5, 2.0)

    # --- Glue catalog (table metadata; no data scanned, no cost) ---
    def list_tables(self) -> list[dict]:
        pages = self._glue.get_paginator("get_tables").paginate(DatabaseName=self._settings.database)
        return [t for page in pages for t in page["TableList"]]

    def get_table(self, name: str) -> dict:
        return self._glue.get_table(DatabaseName=self._settings.database, Name=name)["Table"]

    def get_table_versions(self, name: str, limit: int = 5) -> list[dict]:
        resp = self._glue.get_table_versions(
            DatabaseName=self._settings.database, TableName=name, MaxResults=limit
        )
        return resp["TableVersions"]
    
    def get_partitions(self, name: str) -> list[dict]:
        # Partitions REGISTERED in the catalog. With partition projection,
        # Athena doesn't need these, but catalog-based readers might.
        pages = self._glue.get_paginator("get_partitions").paginate(
            DatabaseName=self._settings.database, TableName=name
        )
        return [p for page in pages for p in page["Partitions"]]


