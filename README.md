# Pipeline Copilot — AI On-Call Assistant for Data Pipelines

When a data pipeline breaks, or quietly loads bad data, Pipeline Copilot investigates and explains the root cause with evidence.

Built with **LangGraph** (agent loop), **MCP** (tool servers for Airflow and Athena), **RAG** (runbooks), **guardrails** (read-only by design), and **memory** (conversation + incident history).

The test target is my own [weather-seismic-pipeline](https://github.com/mahendramahato/weather-seismic-pipeline). The agent should work on any Airflow + Athena pipeline.

---

## 1. The problem

When a pipeline breaks, an on-call engineer normally has to:
1. Dig through Airflow logs
2. Query tables to check the data
3. Search old notes for how it was fixed last time
4. Work out the root cause

Pipeline Copilot does steps 1–4 in minutes and hands back a diagnosis.

**What plain alerting misses:** silent failures. For example, a DAG *succeeds* but loads 0 rows because the upstream API renamed a field. Airflow shows green, but the data is wrong.

---

## 2. Scope

**v1 (this project): investigate and explain.**
- The user asks a question in a terminal chat
- The agent checks Airflow, the data, and runbooks
- It returns a structured diagnosis: root cause, evidence, suggested fix
- **Read-only.** The agent never changes anything. A human applies the fix.

**Future work (not in v1):** executing fixes with human approval, auto-triggering from Airflow failures, opening GitHub PRs, a web UI.

---

## 3. Architecture

### 3.1 Full system

```
╔══════════════════════════════════════════════════════════════════════╗
║  OFFLINE  (run once, and again whenever runbooks change)             ║
║                                                                      ║
║  knowledge/*.md ──► split into ──► embedding ──► Chroma vector DB    ║
║  (runbooks,         chunks         model         (.chroma/)          ║
║   table notes)                                                       ║
╚══════════════════════════════════════════════════════════════════════╝

  ┌────────────────────────────────────────────────────────────────┐
  │  USER (terminal chat)                                          │
  │  "Dashboard shows zero earthquakes today. Real or a bug?"      │
  └───────────────────────────────┬────────────────────────────────┘
                                  ▼
  ┌────────────────────────────────────────────────────────────────┐
  │  LANGGRAPH APP                                                 │
  │                                                                │
  │   ┌──────────────────┐                                         │
  │   │ INPUT GUARDRAIL  │── off-topic / unsafe ──► refusal ─► END │
  │   └────────┬─────────┘                                         │
  │            ▼                                                   │
  │   ┌──────────────────┐  prompt   ┌──────────────────┐          │
  │   │   AGENT NODE     │─────────► │   LLM (API)      │          │
  │   │                  │ ◄──────── │                  │          │
  │   └───┬─────────▲────┘  reply    └──────────────────┘          │
  │       │ tool    │ result                                       │
  │       │ call    │ (errors too — the agent adapts)              │
  │       ▼         │                                              │
  │   ┌─────────────┴────┐                                         │
  │   │   TOOLS NODE     │── local tool: search_runbooks ──► Chroma│
  │   │  (MCP client)    │                                         │
  │   └───────┬──────────┘                                         │
  │           │ done investigating                                 │
  │           ▼                                                    │
  │   ┌──────────────────┐                                         │
  │   │ DIAGNOSIS NODE   │  structured output (Pydantic):          │
  │   │                  │  root cause, evidence, fix, confidence  │
  │   └───────┬──────────┘                                         │
  │           ▼                                                    │
  │   ┌──────────────────┐                                         │
  │   │ OUTPUT GUARDRAIL │  every claim must cite tool evidence    │
  │   └───────┬──────────┘                                         │
  │           ▼                                                    │
  │      answer to user                                            │
  │                                                                │
  │   ┌────────────────────────────────────────────────────────┐   │
  │   │ MEMORY                                                 │   │
  │   │  short-term: checkpointer → memory.sqlite              │   │
  │   │    (this conversation, keyed by thread_id)             │   │
  │   │  long-term: store → memory.sqlite                      │   │
  │   │    (past incident summaries: "has this happened        │   │
  │   │     before?")                                          │   │
  │   └────────────────────────────────────────────────────────┘   │
  └──────────────┬──────────────────────────────┬──────────────────┘
                 │ MCP                          │ MCP
                 ▼                              ▼
  ┌───────────────────────────┐   ┌────────────────────────────────┐
  │  AIRFLOW MCP SERVER       │   │  ATHENA MCP SERVER             │
  │  (Airflow Viewer user)    │   │  (read-only IAM profile)       │
  │                           │   │                                │
  │  • list_dags              │   │  • run_query                   │
  │  • get_dag_runs           │   │      └─ SQL GUARDRAIL:         │
  │  • get_task_instances     │   │         SELECT only, one       │
  │  • get_task_logs          │   │         statement, auto LIMIT  │
  │      └─ trims logs to     │   │  • get_table_schema            │
  │         errors + tail     │   │  • get_table_versions          │
  │                           │   │      (detects schema drift)    │
  └─────────────┬─────────────┘   └───────────────┬────────────────┘
                │ REST API                        │ boto3
                ▼                                 ▼
  ┌───────────────────────────┐   ┌────────────────────────────────┐
  │  Airflow                  │   │  Athena + Glue Data Catalog    │
  │  (Oracle Cloud VM,        │   │  (Parquet on S3)               │
  │   reached via SSH tunnel) │   │                                │
  └───────────────────────────┘   └────────────────────────────────┘
```

**Key idea:** the agent *decides*; the MCP servers *do* and *enforce*. The agent never touches Airflow or Athena directly, and the servers only hold read-only credentials. So even a confused LLM cannot change anything.

### 3.2 LangGraph flow

```
              START
                │
                ▼
       ┌─────────────────┐
       │ input_guardrail │── blocked ──► refusal ──► END
       └────────┬────────┘
                │ ok
                ▼
       ┌─────────────────┐
  ┌──► │      agent      │
  │    └────────┬────────┘
  │             │
  │      wants a tool?
  │       │          │
  │      yes         no
  │       ▼          ▼
  │  ┌─────────┐  ┌───────────┐
  └──│  tools  │  │ diagnosis │
     └─────────┘  └─────┬─────┘
                        ▼
              ┌──────────────────┐
              │ output_guardrail │
              └────────┬─────────┘
                       ▼
             save incident to memory
                       ▼
                      END
```

The **agent ⇄ tools** loop is what makes it agentic: the LLM chooses each next step until it has enough evidence. A max-steps limit stops it from looping forever.

### 3.3 RAG in two halves

```
INGESTION (offline)
  knowledge/schema_drift.md ──► chunk by heading ──► embed ──► Chroma
                                                   + metadata {type: runbook, dag_id: ...}

RETRIEVAL (runtime, via search_runbooks)
  "zero rows loaded, task succeeded" ──► embed ──► nearest chunks ──► top 3 back to agent
```

### 3.4 Example investigation

> "Dashboard shows zero earthquakes today. Real or a bug?"

1. **input_guardrail**: about the pipeline, so it passes.
2. `get_dag_runs("seismic_ingest")`: the last run **succeeded**.
3. `run_query("SELECT count(*) ... WHERE date = today")`: **0 rows**. Suspicious.
4. `get_table_versions("earthquakes")`: the schema changed yesterday, and `mag` was renamed to `magnitude`.
5. `search_runbooks("succeeded but zero rows")`: returns the schema drift runbook.
6. **Memory**: "This happened once before, on 2026-08-14."
7. **diagnosis**:
   - Root cause: upstream USGS field rename (schema drift)
   - Evidence: the DAG run ID, the 0-row count, the schema diff
   - Suggested fix: update the loader's field mapping, then backfill today
   - Confidence: high

---

## 4. Concepts → where they live

| Concept | Where | Phase |
|---|---|---|
| Tools | Airflow, Athena and runbook tools | 1 |
| LangGraph | `graph.py`: nodes, edges, agent loop | 1 |
| MCP | Airflow and Athena MCP servers | 2–3 |
| Guardrails | Input node, SQL guard, read-only creds, output node | 3 |
| RAG | `knowledge/` → Chroma → `search_runbooks` | 4 |
| Memory | Checkpointer + long-term store | 5 |
| Evals | Injected failures + score table | 6 |

### Guardrails (four layers)

| Layer | Guards against | How |
|---|---|---|
| Credentials | Any write, ever | Airflow Viewer role; read-only IAM; Athena workgroup scan limit |
| SQL guard | Destructive or huge queries | Parse SQL (sqlglot), allow one SELECT, force LIMIT |
| Input guardrail | Off-topic or unsafe requests | Classifier node before the agent |
| Output guardrail | Made-up claims | Diagnosis must cite evidence from tool results |

### Diagnosis output (draft)

```python
from pydantic import BaseModel
from typing import Literal

class Evidence(BaseModel):
    source: Literal["airflow", "athena", "runbook", "memory"]
    detail: str                 # e.g. "run_id=scheduled__2026-10-02, state=success"

class Diagnosis(BaseModel):
    root_cause: str
    category: Literal[
        "schema_drift", "bad_upstream_data", "transient_failure",
        "duplicates", "code_bug", "stuck_task", "unknown",
    ]
    evidence: list[Evidence]    # must be non-empty: no evidence, no claim
    suggested_fix: str          # for a human to apply; the agent never executes it
    confidence: Literal["low", "medium", "high"]
```

---

## 5. Tech stack

| Layer | Choice |
|---|---|
| Language | Python 3.11+, `uv` |
| Agent orchestration | LangGraph |
| MCP servers | MCP Python SDK (`FastMCP`) |
| MCP client | `langchain-mcp-adapters` |
| LLM | Configurable via env var (needs tool calling + structured output) |
| Vector DB | Chroma (local) |
| Memory | LangGraph SQLite checkpointer + store |
| SQL parsing | sqlglot |
| AWS | boto3 (Athena, Glue) |
| Tracing | LangSmith or Langfuse |
| Tests | pytest |

### Target pipeline
NOAA + USGS → Python producer → Kafka → Spark Structured Streaming → Parquet on S3 → Glue ETL + Data Catalog → Athena. Orchestrated by Airflow, running in Docker Compose on an Oracle Cloud VM.

Kafka/Spark streaming health is **out of scope for v1**. The agent sees the pipeline through Airflow and Athena.

---

## 6. Phases

Each phase adds **one concept** on top of something that already works.

### Phase 0 — Setup and recon
- **What:** Repo, Python env, `.env`. Confirm Airflow and Athena are reachable from the Mac with read-only credentials.
- **Why:** Most "agent bugs" are really network and auth problems. Fix those before any AI is involved.
- **Done when:** A `curl` to the Airflow API and an AWS CLI Athena query both work with least-privilege credentials.

### Phase 1 — LangGraph + tools
- **What:** One agent with plain Python tools for Airflow (DAG runs, task logs).
- **Why:** Learn the core agent loop (LLM → tool → result → LLM) before adding anything else.
- **Done when:** "When did the weather DAG last succeed?" is answered correctly.

### Phase 2 — MCP
- **What:** Move the Airflow tools into an Airflow MCP server; the agent loads them over MCP.
- **Why:** Separate *deciding* (agent) from *doing* (server). Any MCP client can now reuse the tools.
- **Done when:** Phase 1 answers still work, and the tools are visible in MCP Inspector.

### Phase 3 — Athena MCP + guardrails
- **What:** Athena MCP server (SELECT-only, read-only IAM), plus input and output guardrails.
- **Why:** The agent now touches real data, so safety must be enforced in code, not prompts.
- **Done when:** Row-count questions work, `DROP TABLE` is blocked, and tests cover malicious SQL.

### Phase 4 — RAG
- **What:** Write runbooks in `knowledge/`, ingest them into Chroma, add `search_runbooks`.
- **Why:** The agent learns *how this team fixes things*, not just what the logs say.
- **Done when:** The diagnosis cites the correct runbook.

### Phase 5 — Memory + diagnosis
- **What:** Checkpointer for follow-up questions, long-term store of past incidents, structured `Diagnosis` output.
- **Why:** Real on-call needs context: "Has this happened before?" and "What about yesterday?"
- **Done when:** Follow-ups work across turns, and a repeated incident is recognized.

### Phase 6 — Evals
- **What:** Break the pipeline on purpose in 6 ways and score the agent.
- **Why:** Proof that it works. A score table is what makes this credible.
- **Done when:** The results table below is filled in.

| # | Injected failure | Expected diagnosis | Result |
|---|---|---|---|
| 1 | Upstream field renamed | Schema drift → zero-row load | |
| 2 | Null spike in a column | Bad upstream data | |
| 3 | Upstream API timeout | Transient failure → retry | |
| 4 | Duplicate rows | Missing dedupe / replay | |
| 5 | Bad deploy (code bug) | Task error in logs | |
| 6 | Stuck / long-running task | Hang → clear task | |

**Metrics:** diagnosis accuracy, evidence correctness, time-to-diagnosis.

---

## 7. Repo structure (grows phase by phase)

```
pipeline-copilot/
├── README.md
├── pyproject.toml
├── .env.example
├── src/pipeline_copilot/
│   ├── graph.py          # LangGraph wiring
│   ├── state.py          # graph state
│   ├── guardrails.py     # input / output checks
│   ├── models.py         # Diagnosis, Evidence
│   └── config.py
├── mcp_servers/
│   ├── airflow_server.py
│   └── athena_server.py
├── knowledge/            # runbooks + table notes (markdown)
├── scripts/ingest_knowledge.py
├── evals/
└── tests/
```

---

## 8. Environment variables (`.env.example`)

```
LLM_PROVIDER=
LLM_MODEL=
LLM_API_KEY=

AIRFLOW_BASE_URL=http://localhost:8080   # via SSH tunnel
AIRFLOW_USERNAME=                        # Viewer role only
AIRFLOW_PASSWORD=

AWS_REGION=
AWS_PROFILE=                             # read-only profile
ATHENA_WORKGROUP=
ATHENA_DATABASE=
ATHENA_OUTPUT_S3=

CHROMA_PATH=./.chroma
MEMORY_DB=./memory.sqlite
```

Secrets live only in `.env`, which is git-ignored.

---

## 9. Future work

- Execute approved fixes (LangGraph `interrupt()` for human approval)
- Auto-trigger from Airflow `on_failure_callback`
- Open GitHub PRs for code fixes
- Multi-agent fan-out (logs, data and runbooks investigated in parallel)
- Web UI for incident reports

---

## 10. Working agreement (for Claude in VS Code)

- **I write all the code.** Claude guides with steps and snippets, with comments explaining *why*.
- Every phase starts with a plain-language summary: **what** we're building and **why**.
- One phase at a time. Wait for my results before moving on.
- When I'm confused, slow down and use small concrete examples.
- I commit and push myself. No Claude attribution (`Co-Authored-By` etc.) in commits or PRs.
- Never put secrets in code or commits.
