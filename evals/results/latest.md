# Eval results (20261006-230633 UTC)

```
✓ schema_drift_magnitude       got=schema_drift       conf=medium calls=10 $0.14 
✓ null_spike_station           got=bad_upstream_data  conf=medium calls=11 $0.18 
✓ transient_sync_retry         got=transient_failure  conf=high   calls=17 $0.31 
✓ duplicate_replay             got=duplicates         conf=high   calls=17 $0.28 
✓ glue_code_bug                got=code_bug           conf=high   calls=14 $0.19 
✓ stuck_task                   got=stuck_task         conf=high   calls=10 $0.16 
✓ healthy_control              got=no_problem_found   conf=high   calls=18 $0.23 

Correct: 7/7  (runbook-covered 3/3, not covered 4/4)
Grounded: 7/7   Confidently wrong: 0
Avg tool calls: 13.9   Avg agent-loop cost: $0.21
```
