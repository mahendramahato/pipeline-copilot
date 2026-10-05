# Eval log

## Case 1: Silent curation failure (real, found 2026-10-05)
Question: "Why is the curated data stuck at 2026-09-30?"
Truth: raw tables use partition projection; Glue job reads via the catalog, whose
raw partitions stop at 09-30 -> job reads 0 rows -> writes nothing -> succeeds.

| Date | Version | Detected? | Impact correct? | Root cause | Notes |
|------|---------|-----------|-----------------|------------|-------|
| 2026-10-05 | Phase 3 (no RAG) | yes | yes | no: blamed output path; "wrong raw input" was 1 of 3 guesses | checked projection on curated tables only |
