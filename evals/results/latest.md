# Eval results (20261007-020620 UTC)

```
✓ schema_drift_magnitude       got=schema_drift       conf=medium calls=11 $0.08 
✓ null_spike_station           got=bad_upstream_data  conf=medium calls=12 $0.09 
✓ transient_sync_retry         got=transient_failure  conf=high   calls=12 $0.08 
✓ duplicate_replay             got=duplicates         conf=high   calls=16 $0.11 
✓ glue_code_bug                got=code_bug           conf=medium calls=14 $0.08 
✓ stuck_task                   got=stuck_task         conf=high   calls=12 $0.07 
✓ healthy_control              got=no_problem_found   conf=high   calls=17 $0.10 

Correct: 7/7  (runbook-covered 3/3, not covered 4/4)
Grounded: 7/7   Confidently wrong: 0
Avg tool calls: 13.4   Avg agent-loop cost: $0.09
```
