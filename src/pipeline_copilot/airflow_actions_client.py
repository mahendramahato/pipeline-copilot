"""Airflow client that can CHANGE things: clear a task, trigger a run.

Kept separate from the read-only AirflowClient on purpose. Only the actions MCP
server imports this, and it logs in as a different Airflow user
(AIRFLOW_ACTIONS_USERNAME) whose role allows editing just this DAG's runs.
"""
import httpx

from pipeline_copilot.airflow_client import AirflowClient, _q


class AirflowActionsClient(AirflowClient):

    def _post(self, path: str, body: dict) -> dict:
        if self._token is None:
            self._login()

        def send() -> httpx.Response:
            return self._http.post(path, json=body, headers={"Authorization": f"Bearer {self._token}"})

        resp = send()
        if resp.status_code == 401:
            self._login()
            resp = send()
        resp.raise_for_status()
        return resp.json()

    def get_dag_run(self, dag_id: str, run_id: str) -> dict:
        return self._get(f"/api/v2/dags/{_q(dag_id)}/dagRuns/{_q(run_id)}")

    # --- Re-run one task (and everything downstream of it) in an existing run ---
    # reset_dag_runs puts a finished run back to "queued" so the scheduler picks it up.
    def clear_task(self, dag_id: str, run_id: str, task_id: str) -> dict:
        return self._post(f"/api/v2/dags/{_q(dag_id)}/clearTaskInstances", {
            "dry_run": False,
            "dag_run_id": run_id,
            "task_ids": [task_id],
            "only_failed": False,
            "reset_dag_runs": True,
            "include_downstream": True,
            "include_upstream": False,
        })

    # --- New run with conf (the DAG's backfill param) ---
    # logical_date=None: Airflow 3 runs without a data interval, like "Trigger DAG w/ config".
    def trigger_run(self, dag_id: str, run_id: str, conf: dict, note: str) -> dict:
        return self._post(f"/api/v2/dags/{_q(dag_id)}/dagRuns", {
            "dag_run_id": run_id,
            "logical_date": None,
            "conf": conf,
            "note": note,
        })
