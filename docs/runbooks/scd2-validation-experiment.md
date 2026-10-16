# Rejected SCD2 validation optimization

Combining three target-invariant Spark actions into one reduced jobs but did not
provide sufficient end-to-end benefit on the measured retained commerce fixture.
Keep the existing production validator. The candidate remains an executable
experiment artifact, not a runtime flag or an adopted job change.

The [report](../evidence/scd2-validation-probe-2026-10-16.json) records the decision,
14 correctness cases, three matched stage pairs, timings, job counts, source/image
identity and cleanup. The exact hash-bound
[probe source](../evidence/scd2-validation-probe-2026-10-16.py.txt) is retained so the
rejected algorithm and measurement can be reviewed and reproduced. Its `.py.txt`
suffix preserves the exact measured bytes as historical evidence, outside runtime
source discovery. Copy it to `probe.py` for execution as described below.

## Hypothesis and decision boundary

`validate_target` in the existing
[customer SCD2 job](../../jobs/spark/commerce_customer_scd2.py) separately counts
invalid current-version groups, malformed periods and discontinuities. The
candidate calculates the same period/discontinuity predicates using a lag window,
then groups their flags and current-version counts by customer before one bounded
violation count. It introduces no cache and changes no merge/publication logic.

Before inspecting paired results, the operator recorded an adoption threshold:
all three pairs must improve stage time, and median reductions in both stage and
complete process time must reach 10%, with correctness and warehouse checks passing.
This threshold is recorded in the evidence. It is a local engineering decision,
not a statistical significance test or a production objective.

## Equivalent input and correctness

The baseline runtime was `e2debb5b424bf3b9fb7aef9febd18400ca38d2ef` with the
cached Spark 3.5.9 / Iceberg 1.11.0 image and the existing Compose configuration.
The independently restored warehouse contained 330,000 customer-history versions,
10,000 current customers and the already accepted batch `24e47c2864bd8fb4`.
Both modes read the same selected 10,000 silver rows and the same compacted current
history snapshot `7283836129631754092`. All source and other stage code was identical.

A separate correctness JVM compared baseline/candidate acceptance on 14 frames:
empty, current-only, continuous and multiple-customer history; missing/duplicate
current versions; reversed/zero-length periods; closed-current/open-historical
rows; gaps and overlaps; a null current flag; and an existing null-customer behavior.
All expected outcomes matched. These tests preserve the existing validator's
behavior; they do not claim every malformed history is detected.

Each measurement then started a **fresh Spark application/container** and invoked
the whole existing `apply_customer_history` stage. Only its `validate_target`
function was substituted in candidate mode. The selected batch was an unchanged
replay, so all six stages reported 10,000 selected/current rows, 330,000 target
rows before/after, zero changed rows and zero inserted rows. Every run's file and
snapshot inventories matched the preserved warehouse. No MERGE or checkpoint
reset was used to manufacture equivalent inputs.

## Three pairs

| Pair | Order | Baseline stage s | Candidate stage s | Baseline process s | Candidate process s |
|---|---|---:|---:|---:|---:|
| 1 | baseline, candidate | 6.148 | 5.750 | 11.575 | 10.761 |
| 2 | candidate, baseline | 5.862 | 5.500 | 10.628 | 10.632 |
| 3 | baseline, candidate | 5.698 | 5.590 | 10.571 | 10.561 |
| Median | | 5.862 | 5.590 | 10.628 | 10.632 |

Jobs attributed to the full stage fell from 47 to 37 in every pair. Stage median
improved 4.66%, while process median worsened approximately 0.03%. The candidate
therefore failed both 10% median thresholds. Fewer Spark actions/jobs alone are
not evidence of a worthwhile user-visible improvement.

Stage time surrounds `apply_customer_history`; process time surrounds the Compose
job invocation and also includes container/JVM setup and shutdown. Their difference
is not an isolated JVM-startup measurement. The three pairs used separate JVMs but
shared catalog/object-store services and host caches. Order was counterbalanced,
not randomized, and only unchanged replay on the compacted fixture was measured.
This does not establish changed-batch MERGE, full nine-stage runner throughput,
failure-recovery performance, growth beyond this fixture or production capacity.

## Reproduce within the recorded boundary

Use the pinned baseline runtime, the same verified restored fixture and exclusive
ownership of its project/volumes. Confirm the selected batch is already committed
and the current snapshot matches before running this fixture-specific probe. Keep
original volumes, continuation state and cold archives untouched.

Copy the published probe to `artifacts/scd2-validation-probe/probe.py` in that
baseline worktree. Start only the existing five core services with fresh host ports
and the memory overlay. Run `spark-maintenance` with its command overridden to
`/opt/spark/bin/spark-submit /opt/lakehouse/artifacts/scd2-validation-probe/probe.py`.
Pass these container environment values explicitly:

```text
VALIDATION_PROBE_MODE=correctness
VALIDATION_PROBE_REPORT=/opt/lakehouse/artifacts/scd2-validation-probe/correctness.json
```

Require all 14 cases to pass. Repeat the same one-off command in the documented
AB/BA/AB order with modes `baseline` or `candidate` and unique report/log paths.
Use `--rm --no-deps --pull never` and the exact owned project. Time each complete
process externally; the probe records stage duration and Spark job-group counts.
Check file/snapshot inventories against the starting state after every run.
Preserve failed attempts instead of overwriting their reports or retrying for a
better timing sample.

After the experiment, collect logs and remove only that project's containers and
network without deleting volumes. This run preserved all six restored and six
original volumes; 14 unrelated container identities were unchanged. The production
job is unchanged, so no new recovery boundary or optimized-path support is claimed.
