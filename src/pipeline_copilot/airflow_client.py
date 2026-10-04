"""Thin read-only client for the Airflow 3 REST API (/api/v2)."""
from urllib.parse import quote

import httpx

from pipeline_copilot.config import Settings


# --- URL-safe path pieces ---
# Run IDs look like "scheduled__2026-10-03T00:30:00+00:00". Characters such as
# ":" and "+" must be percent-encoded when they're part of a URL path, or the
# server may read a different ID than we meant. safe="" encodes everything.
def _q(value: str) -> str:
    return quote(value, safe="")


class AirflowClient:
    """Read-only on purpose: there are no POST/PATCH/DELETE methods here,
    so even a bug in the agent can't ask this class to change Airflow.
    (The agent_viewer role blocks writes too: two independent layers.)"""

    def __init__(self, settings: Settings):
        self._settings = settings
        # One reusable connection pool. base_url means we only write paths
        # like "/api/v2/dags". timeout stops a hung tunnel from hanging the agent.
        self._http = httpx.Client(base_url=settings.airflow_base_url, timeout=30.0)
        self._token: str | None = None

    # --- Authentication ---
    # Exchange username + password for a JWT. Called lazily on the first
    # request, and again if Airflow says the token expired (401).
    def _login(self) -> None:
        resp = self._http.post(
            "/auth/token",
            json={
                "username": self._settings.airflow_username,
                "password": self._settings.airflow_password,
            },
        )
        resp.raise_for_status()
        self._token = resp.json()["access_token"]

    # --- Every API call goes through here ---
    # Retry exactly once on 401 (expired token). Any other error, like 404 for
    # an unknown DAG, raises immediately with Airflow's status code.
    def _get(self, path: str, params: dict | None = None) -> dict:
        if self._token is None:
            self._login()

        def send() -> httpx.Response:
            return self._http.get(
                path,
                params=params,
                headers={"Authorization": f"Bearer {self._token}"},
            )

        resp = send()
        if resp.status_code == 401:
            self._login()
            resp = send()
        resp.raise_for_status()
        return resp.json()

    # --- Read-only API methods ---
    def list_dags(self) -> list[dict]:
        return self._get("/api/v2/dags", params={"limit": 100})["dags"]

    def get_dag_runs(self, dag_id: str, limit: int = 10) -> list[dict]:
        # "-run_after" = newest first (the "-" means descending)
        return self._get(
            f"/api/v2/dags/{_q(dag_id)}/dagRuns",
            params={"order_by": "-run_after", "limit": limit},
        )["dag_runs"]

    def get_task_instances(self, dag_id: str, run_id: str) -> list[dict]:
        return self._get(
            f"/api/v2/dags/{_q(dag_id)}/dagRuns/{_q(run_id)}/taskInstances"
        )["task_instances"]

    def get_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> dict:
        # Returns the raw response for now. We'll inspect its shape in the
        # test below before deciding how to trim it for the LLM in Step 4.
        return self._get(
            f"/api/v2/dags/{_q(dag_id)}/dagRuns/{_q(run_id)}"
            f"/taskInstances/{_q(task_id)}/logs/{try_number}",
            params={"full_content": "true"},
        )
