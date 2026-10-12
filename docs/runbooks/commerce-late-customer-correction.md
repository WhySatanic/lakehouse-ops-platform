# Late-effective customer correction through the reference stack

This opt-in drill extends the three linked snapshots with an observation that arrives
after February 3 but is effective at February 1 noon. It uses real conditional MinIO
landing, production bronze and customer silver jobs, the existing opt-in SCD2 rebuild,
and an independent literal Trino oracle. The original forward runner is unchanged.

## Run

Use the fresh isolated stack setup in [linked acceptance](commerce-linked-acceptance.md).
Choose unused ports and a unique `lakehouse-ops-linked...` project, start only its five
reference services, and keep competing commerce writers stopped. Add the explicit flag:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_linked_acceptance `
  --late-customer-history --output artifacts/linked-history-fresh `
  --s3-endpoint-url http://localhost:19400 --server http://localhost:18400
```

The default driver still checks three snapshots, 1000/1500/2200 cents, three checkpoints,
current-mart replay and compute-free idle. The new flag then lands a separate complete
synthetic source fixture with a hash-bound manifest and earlier effective timestamp.
Conditional landing replay creates no new objects. Only bronze and customer silver are
processed for this observation; its order/payment fixture rows are not promoted to silver
or published into current revenue. This is explicit history maintenance, not a fourth
normal batch run or an automatically scheduled backfill.

The rebuild reads **all** four retained silver customer observations. Its exact expected
name/period history is:

| Name | From, UTC | Until, UTC | Current |
| --- | --- | --- | --- |
| Alice | February 1, 00:00 | February 1, 12:00 | No |
| Alice Corrected | February 1, 12:00 | February 2, 00:00 | No |
| Alice Updated | February 2, 00:00 | Open | Yes |

The first rebuild must add exactly one Iceberg history snapshot. A second rebuild must
preserve that snapshot count and the same literal periods. Both attempts independently
verify current daily revenue, latest orders and the February 3 publication identity.
Current revenue snapshots and the exact processing-checkpoint bytes must remain unchanged.

`linked.json` retains `late_customer_landing` and `late_customer_correction`, including
the oracle and two distinct maintenance run IDs. The shared SQLite journal retains stage
and container ownership and terminal ready/failed events. Neither maintenance attempt
records a checkpoint. Failed rebuilds/oracles retain their evidence and return failure;
an already published Iceberg snapshot is not rolled back. Earlier three checkpoints stay
authoritative. Inspect and retry with complete input, never reset them for a green report.

## Limits

This is a bounded one-customer, single-writer acceptance, with batch time as effective
time. It does not implement conflicting revisions at the same timestamp, deletions,
business-event-time extraction, incomplete-retention detection, automatic backfill
planning/checkpointing, concurrent writers or failure inside an Iceberg commit.
The journal remains diagnostic rather than a transaction coordinator. Retain volumes
and reports, and stop only the experiment's scoped reference services afterwards.

The compute-free idle observation is **before** landing the late fixture. Afterwards,
that fixture remains pending in the normal planner because maintenance deliberately
does not checkpoint it. Do not resume the normal forward pipeline on this diagnostic
catalog: its older changed customer would be rejected. This drill is not a completed
automatic late-batch ingestion path, and must use a disposable isolated experiment.

Check retained real evidence without starting compute or modifying artifacts:

```powershell
$env:LAKEOPS_HISTORY_CORRECTION_DIRECTORY = (Resolve-Path artifacts/linked-history-fresh).Path
$env:LAKEOPS_RUN_HISTORY_CORRECTION_SMOKE = '1'
uv run --frozen pytest tests/integration/test_commerce_retained_history_correction.py --no-cov
```

This requires six ready journal attempts, three normal checkpoints and two separate
maintenance runs with five owned/completed containers in total. Missing proof fails
the opt-in check; default quality runs skip it rather than invent real-stack evidence.

See [rebuild semantics](commerce-customer-history-rebuild.md) for production-mode
requirements and the separate Spark/Iceberg invalid-source and return-to-prior-state checks.

## Local evidence, 2026-10-12

Fresh `lakehouse-ops-linked-history-20261012-v1` completed all three full pipelines,
current publication/replay and the additional real S3-to-silver late observation.
The rebuild reported four source observations, three history periods, `action: replace`,
then `action: replay`. Trino confirmed the exact literal periods and 2200 captured cents;
history gained one snapshot, current revenue snapshots and checkpoint bytes stayed fixed.

The read-only retained-evidence smoke passed: six ready attempts, three checkpoints,
two maintenance run IDs and five owned/completed maintenance containers. Evidence hashes
were unchanged. Only this experiment's five reference services were stopped; its volumes
and logs remain, with no running project container. No automatic late-batch processing,
release-candidate or transaction-failure evidence is claimed.

Report: `artifacts/linked-history-20261012-v1/linked.json`, SHA256
`bf50fd8e287d97508dac0e07a30117a26f3952cfe18fe7c614c355f38eb2b0f5`.
Fresh main-based local checks: 1033 passed, 19 opt-in skips, 92.18% coverage, all policy,
contract/image-lock/lint gates, plus three isolated Docker smokes. No version/release change.
