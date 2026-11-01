# Project notes (read this first in a new session)

## What it is
AI on-call agent for the weather-seismic pipeline: investigates (read-only tools), produces a
verified Diagnosis, proposes a typed fix, waits for human approval, executes, validates.
Live: pipeline-copilot.duckdns.org (Oracle VM, docker compose, behind the pipeline's Caddy).

## Layout / key files
- `src/pipeline_copilot/graph.py`: the LangGraph. Nodes: input_guardrail → agent ⇄ tools →
  [triage] → diagnose → verify_diagnosis → plan → await_approval (interrupt) → execute →
  validate → remember. Prompts live here.
- `graph_state.py`: state = messages + intent, diagnosis, plan, approval, remediation.
- `models.py`: Diagnosis, Evidence, ProposedAction (Pydantic, structured output).
- `resources.py`: host + per-container memory/CPU/disk (/proc + Docker stats), thresholds
  (`checks`), 7-day history file written by the monitor (RESOURCE_HISTORY). Tools in ops_server:
  `get_host_resources`, `get_resource_history`.
- `remediation.py`: action CATALOG + `check_action` / `vet_plan` (pure; shared with actions server).
- `mcp_servers/`: airflow, athena, knowledge, ops (read-only), actions (writes; graph-only tools).
- `airflow_actions_client.py`: the only writing Airflow client (clear task, trigger run).
- `runtime.py`: starts MCP servers; `run_turn` / `resume_turn` / `pending_approval`; events.
- `api.py`: FastAPI SSE. `/api/chat`, `/api/threads/{id}/decision` (approve/reject).
- `cli.py`: terminal chat, prompts `Apply this fix? [y/N]`.
- `monitor.py`: 30-min deterministic checks → AI investigation → alert (never self-approves).
- `frontend/src/`: `Turn.jsx`, `DiagnosisCard.jsx`, `FixCard.jsx` (approve/reject, outcome).
- `evals/`: simulated World + fake tools (incl. actions, resources) + 12 scenarios; `run.py` auto-approves.
- `tests/test_remediation.py`: fix loop end to end with a scripted fake model (no LLM).

## Events streamed to UI/CLI
tool_call, tool_result, answer, escalated, diagnosis, plan, approval_required, progress,
approval, remediation, memory, error.

## Config switches
`ACTIONS_ENABLED` (default false), `AIRFLOW_ACTIONS_USERNAME/PASSWORD`, `DOCKER_ACTIONS_URL`,
`ACTIONS_AUDIT_LOG`, `VALIDATION_POLL_SECONDS`, `VALIDATION_TIMEOUT_SECONDS`,
`CONTAINER_SETTLE_SECONDS`, `RESOURCE_HISTORY`.

## Status (2026-11-01)
- Remediation loop built; UI approve flow checked with a mock server.
- Resource forensics (memory/CPU/disk + history) built; 109 tests pass.
- Not yet done on the VM: create Airflow role/user `agent_operator`, set ACTIONS_ENABLED=true.
- Not yet run: the LLM eval suite with the new scenarios (`uv run python -m evals.run`).

## Open items / ideas
- On the VM: set RESOURCE_HISTORY, check Docker stats come through the read-only proxy.
- Give soundly / momentum-scanner memory and CPU limits so one app can't starve the pipeline.
- Verify tecnativa proxy `ALLOW_RESTARTS=1` with `POST=0` really allows only restart (expect 403 on others).
- Approve/Reject buttons in the Slack/Discord alert.
- Over-investigation on healthy cases (17–18 tool calls).
