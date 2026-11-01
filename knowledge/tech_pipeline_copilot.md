# Technology: how Pipeline Copilot works (LangGraph, MCP, RAG, guardrails, memory)

## What is Pipeline Copilot?
An AI on-call assistant for this data pipeline. Asked why something looks wrong, it investigates
like an on-call engineer: reads Airflow runs and logs, queries the data lake, checks runbooks and
past incidents, and returns a structured diagnosis whose evidence is verified against real tool
output. Then it can fix the problem with a human in the loop: it proposes a typed fix, waits for
the owner's approval, runs exactly that, and verifies the fix worked. The model itself only
reads; it never holds a tool that changes anything.

## What is LangGraph and how the agent is built
LangGraph builds an AI agent as a state graph: nodes (steps) that read and update a shared state,
connected by edges. Here: input_guardrail -> agent (with a tools loop) -> [triage] -> diagnose ->
verify_diagnosis -> plan -> await_approval -> execute -> validate -> remember. Triage escalates a
plain question into a diagnosis when the answer surfaced a real problem. In the agent loop the model (Claude Opus by default) decides which
tool to call next, reads the result, and repeats until it can answer.

## What is MCP (Model Context Protocol)?
MCP is an open standard for giving AI models tools. Tools live in separate MCP servers; any MCP
client (this agent, Claude Code, ...) can discover and call them. Pipeline Copilot runs five
servers. Four are read-only: Airflow (DAG runs, task instances, task logs), Athena (tables, schemas, versions,
registered partitions, SQL queries, Glue job runs and logs), Knowledge (runbook search, past
incidents) and Ops (the live local lake, container status and logs, dashboard health). The fifth,
Actions, can clear a task, trigger a backfill run or restart a container; its tools are hidden
from the model and called only by the graph after approval. Each server holds its own
credentials, so the agent process never sees them.

## How fixes work (remediation)
1. The diagnosis may propose actions from a fixed catalog: clear_task(run_id, task_id) to re-run
   a failed or stuck task, backfill_date(date) to re-curate one past day, restart_container(name).
   Nothing else exists: no free-form commands or SQL writes.
2. Code vets each proposal: exact run_id format, a known task, a past day within 30 days, an
   allow-listed container, at most 3 actions, none when confidence is low or no problem was found.
3. The graph pauses (LangGraph interrupt) until the owner approves or rejects in the web UI or CLI.
   The health monitor never approves its own fixes; its alert says a fix is waiting.
4. Approved actions run through the Actions server, which re-checks them, uses a separate
   Airflow user and a restart-only Docker proxy, and writes every attempt to an audit log.
5. Validation: wait for the Airflow run to finish, count curated rows for the re-curated day,
   check a restarted container is running, re-run all health checks. Outcome: resolved,
   partially_resolved or not_resolved. The outcome is saved with the incident.
Code bugs (e.g. a broken Glue script) get no automated action: a human must fix the code first.

## The health monitor
Every 30 minutes the hosted instance runs free, deterministic checks: is each streaming feed
writing, is every weather station current, are the containers running, is the dashboard up,
did last night's DAG run succeed, is yesterday curated. Only when a check newly fails does it run
an AI investigation (at most a few per day) and send one alert with the root cause, impact and
fix; when the check passes again it announces recovery.

## What is RAG and how it is used here
RAG (retrieval-augmented generation) lets a model answer from documents it was not trained on.
These knowledge docs are split by section, embedded locally into vectors, and stored in Chroma.
When the agent sees a symptom or a question like this one, it calls search_runbooks; the most
similar sections come back and the model answers from them, citing the source. It is "agentic"
RAG: the model decides when to search.

## Guardrails and memory
- Input guardrail: a small model (Claude Haiku) rejects off-topic or unsafe requests first.
- SQL guardrail: queries are parsed (sqlglot); only one SELECT on known tables, with a LIMIT.
- Read-only access for investigation: Airflow Viewer role, read-only IAM, Athena scan cap.
  Writes only through the Actions server, after human approval, with ACTIONS_ENABLED=true.
- Output guardrail: every evidence quote in a diagnosis must appear verbatim in a tool output.
- Memory: conversations are saved (SQLite checkpointer); verified diagnoses are saved as
  incidents so "has this happened before?" can be answered.

## How it was evaluated
Seven failures injected into a simulated copy of the pipeline (schema drift, a failing station,
a transient retry, duplicates, a bad deploy, a stuck task, and a healthy control) were all
diagnosed correctly with grounded evidence. It also found a real silent failure in the live pipeline.
The suite now also scores fixes: three more scenarios need an action (a crashed container, a Glue
concurrency clash, a day missed after an S3 timeout), and every proposed fix is auto-approved,
run against the simulated world and validated.
