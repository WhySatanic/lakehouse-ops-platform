# Observed 100-snapshot budget refusal

[Run 38969869281](https://github.com/WhySatanic/lakehouse-ops-platform/actions/runs/38969869281)
failed on October 17, 2026 at immutable source
`6459ed9595b2a387c7ff60f36727484160efedaa`. The profile subprocess exhausted its
19500-second limit (325 minutes). This is a failed experiment with a verified
90-snapshot prefix, **not a successful 100-snapshot run** or a universal maximum
of 90 snapshots. The [public result](../evidence/customer-history-100-budget-result-2026-10-17.json)
records the exact run/artifact identity, raw evidence hashes, timings and limits.

The workload admitted 100 chronological full snapshots with revisions 0 through 99,
10000 customers and 100 canonical orders per snapshot. The hosted runner reported
four logical CPUs and 16766414848 B RAM. Images were built/preloaded before the
measurement; services and catalog were fresh. This is not cold-download evidence,
a production capacity statement or a repeated comparison of optimizations.

## Verified completed prefix

The retained report and checkpoint contain 90 completed batches, February 1 through
May 1. A separate read-only audit rechecked every input/manifest hash, canonical
customer ID/name/revision, literal history period and current flag, detail/daily/current
SQL revenue, all nine stages, observed memory, idle files/snapshots and the retained
object-reference/size accounting. Every completed sample has 100 orders and 277225
cents revenue. Sample 90 records 900000 history versions and 10000 current customers.
All 1195 extracted files remained unchanged during the audit.

| Snapshot | History rows | Runner seconds | SCD2 seconds | Warehouse payload B |
|---|---:|---:|---:|---:|
| 1 | 10000 | 175.177 | 16.822 | 1964846 |
| 25 | 250000 | 184.007 | 23.456 | 82450984 |
| 50 | 500000 | 196.080 | 27.458 | 178114449 |
| 75 | 750000 | 203.335 | 30.489 | 285438263 |
| 90 | 900000 | 211.072 | 32.938 | 355526825 |

The 90 completed runner calls total 17349.287 seconds; their median is 194.894
seconds. The 19500.021-second failed profile step also includes observations and
work on the incomplete batch. The difference is not a measured observer-only cost.
The largest sampled stack memory is 4449873428 B, not an exact simultaneous peak.
History depth, catalog lifetime and cache state change together; these timings do
not isolate a bottleneck or establish a speedup against the earlier 50-snapshot run.

Sample 90 reconciles 355526825 B across 5363 object versions: current data
153818073 B, retained-only data 110150894 B and latest objects outside selected
references 91557858 B. Delete files and noncurrent S3 versions contribute zero.
Unselected objects are not classified as orphan. Payload is not physical disk usage
or billing. This is the **last completed observation**, not a final catalog audit
after the timeout.

## Incomplete batch and cleanup

The checkpoint still contains 90 entries. The retained active intent identifies
May 2 batch `0f45ea4572fab65f`; its manifest hash matches the generated sample-91
fixture and is absent from the checkpoint. The active intent does not establish
whether an Iceberg commit completed. There is no verified recovery or final SQL
observation for that batch. Do not infer an in-commit failure from a killed profile
owner or replace this result with a post-stage failure drill.

Startup, observer setup, service-log capture and Compose stop returned zero. Stop
removed the base services but reported the network still in use; the experiment
records `demo network has attached containers; network retained`. Cleanup is
**incomplete**. There is no retained immutable-ID inspection proving the attached
container's ownership/liveness or its final commit outcome. Hosted-runner disposal
is not a proven scoped cleanup or recovery mechanism. No local containers were started to audit this artifact.

The next useful investigation is bounded observation and containment of a timed-out
attempt's owned active stage, with immutable identity and ownership evidence. Do
not increase the budget or blindly dispatch another 100-snapshot attempt. Any later
optimization needs matched measurements and the existing correctness oracles.

## Reproduce the evidence audit

Obtain artifact `12087127273` (`customer-history-experiment`) from the exact failed
run above. Independently confirm the Actions API run/artifact source SHA before
interpreting its JSON. The API digest in the public result is recorded metadata;
it is not a claim that this audit authenticated the compressed download.
Use the extracted directory containing `customer-history-ci/` and
`customer-history-runner/`, and run without Python's `-O` flag:

```powershell
uv run --frozen python docs/evidence/audit-customer-history-100-prefix.py artifacts/downloaded-history100
uv run --frozen python docs/evidence/check-customer-history-100-prefix-faults.py artifacts/downloaded-history100
```

The [historical prefix auditor](../evidence/audit-customer-history-100-prefix.py)
admits only this failed source/workload, 90 completed entries and its active next
batch. It reports `failed_experiment_completed_prefix_verified`. It reuses the
full auditor's storage accounting and leaves the complete-run acceptance unchanged.
The [counterchecks](../evidence/check-customer-history-100-prefix-faults.py) reject
eight in-memory semantic substitutions, including sample 90 period/revenue and
storage accounting, without editing the artifact. The full `--snapshots 100` auditor
correctly refuses this failed/running report. Neither audit turns the prefix into
successful completion, final catalog recovery or distributed exactly-once.
