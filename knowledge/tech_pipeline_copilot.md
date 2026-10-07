# Technology: how Pipeline Copilot works (LangGraph, MCP, RAG, guardrails, memory)

## What is Pipeline Copilot?
An AI on-call assistant for this data pipeline. Asked why something looks wrong, it investigates
like an on-call engineer: reads Airflow runs and logs, queries the data lake, checks runbooks and
past incidents, and returns a structured diagnosis whose evidence is verified against real tool
output. It is read-only: it never changes anything.

## What is LangGraph and how the agent is built
LangGraph builds an AI agent as a state graph: nodes (steps) that read and update a shared state,
connected by edges. Here: input_guardrail -> agent (with a tools loop) -> diagnose ->
verify_diagnosis -> remember. In the agent loop the model (Claude Opus by default) decides which
tool to call next, reads the result, and repeats until it can answer.

## What is MCP (Model Context Protocol)?
MCP is an open standard for giving AI models tools. Tools live in separate MCP servers; any MCP
client (this agent, Claude Code, ...) can discover and call them. Pipeline Copilot runs four
servers: Airflow (DAG runs, task instances, task logs), Athena (tables, schemas, versions,
registered partitions, SQL queries, Glue job runs and logs), Knowledge (runbook search, past
incidents) and Ops (the live local lake, container status and logs, dashboard health). Each
server holds its own read-only access, so the agent process never sees credentials.

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
- Read-only access everywhere: Airflow Viewer role, read-only IAM, Athena scan cap.
- Output guardrail: every evidence quote in a diagnosis must appear verbatim in a tool output.
- Memory: conversations are saved (SQLite checkpointer); verified diagnoses are saved as
  incidents so "has this happened before?" can be answered.

## How it was evaluated
Seven failures injected into a simulated copy of the pipeline (schema drift, a failing station,
a transient retry, duplicates, a bad deploy, a stuck task, and a healthy control) were all
diagnosed correctly with grounded evidence. It also found a real silent failure in the live pipeline.
