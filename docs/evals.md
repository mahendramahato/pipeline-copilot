# Eval log

## Case 1: Silent curation failure (real, found 2026-10-05)
Question: "Why is the curated data stuck at 2026-09-30?"
Truth: raw tables use partition projection; Glue job reads via the catalog, whose
raw partitions stop at 09-30 -> job reads 0 rows -> writes nothing -> succeeds.

| Date | Version | Detected? | Impact correct? | Root cause | Notes |
|------|---------|-----------|-----------------|------------|-------|
| 2026-10-05 | Phase 3 (no RAG) | yes | yes | no: blamed output path; "wrong raw input" was 1 of 3 guesses | checked projection on curated tables only |

| 2026-10-06 | Phase 4 (RAG) | yes | yes | YES: checked registered partitions on both raw tables, explained projection (Athena) vs catalog (Glue) correctly, cited runbook_curated_data_missing.md | searched runbooks at first symptom and again before concluding |

⚠️ Caveat: the runbook was written after this incident was understood, so this result
measures retrieval + application of known knowledge, not discovery. The generalization
test is Phase 6: injected failures with runbooks written beforehand.

called get_registered_partitions on curated only, never on raw.

| 2026-10-05 | Phase 3 + partition tools | yes | yes | no: fetched raw_weather registered partitions (stop 09-30) but dismissed them, reasoning about Athena projection, not Spark/Glue catalog reads | had the decisive evidence and explained it away |

## Phase 6: simulated scenario suite (2026-10-06)
Seven failures injected into a simulated pipeline (fake tools with the real schemas,
real SQL guardrail, real SQL in DuckDB). Runbooks for the 4 "not covered" cases were
deliberately NOT written, so those test reasoning without help.
Run: `uv run python -m evals.run` (details: evals/results/latest.md).

| Scenario | Expected | Got | Confidence | Tool calls | Agent-loop cost |
|---|---|---|---|---|---|
| schema_drift_magnitude | schema_drift | ✓ | medium | 10 | $0.14 |
| null_spike_station (no runbook) | bad_upstream_data | ✓ | medium | 11 | $0.18 |
| transient_sync_retry (no runbook) | transient_failure | ✓ | high | 17 | $0.31 |
| duplicate_replay (no runbook) | duplicates | ✓ | high | 17 | $0.28 |
| glue_code_bug | code_bug | ✓ | high | 14 | $0.19 |
| stuck_task (no runbook) | stuck_task | ✓ | high | 10 | $0.16 |
| healthy_control | no_problem_found | ✓ | high | 18 | $0.23 |

**7/7 correct, 7/7 grounded, 0 confidently wrong, avg $0.21 (agent loop only).**

Caveats: one run per scenario (LLMs vary; confirm with `--repeat 3`), scenarios were
written by the same person who built the agent, and scoring is keyword-based.
Open issue: simple or healthy cases still use 17-18 tool calls, so the agent over-investigates.
