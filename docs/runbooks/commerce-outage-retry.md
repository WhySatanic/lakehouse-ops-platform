# Retained commerce state after an observed Docker outage

One observed local outage interrupted the 33rd full-snapshot batch of the
[customer history depth attempt](customer-history-experiment.md#interrupted-50-snapshot-attempt-2026-10-16).
After Docker became available, the existing runner completed that same hash-bound
batch against the retained warehouse and catalog. The
[recorded evidence](../evidence/customer-history-outage-retry-2026-10-16.json)
contains before/after literal customer-period SQL and query IDs, checkpoint entries,
snapshot/file inventories, retry/idle results, original report hashes and journal
tail. The published JSON is derived evidence with the working state path normalized;
the exact raw report is retained separately with its SHA256. Raw logs, fixtures and
volumes remain local operator evidence, not a public CI artifact or a tested backup.

This is recovery from one observed interruption with an unknown cause. It is not a
new runtime feature, a controlled host/power-loss drill, proof of failure inside an
Iceberg commit, a backup restore or completion of the 50/100-snapshot experiment.
Use the [controlled active Spark drill](commerce-active-spark.md) for a reproducible
scoped interruption. Do not stop a shared Docker engine to recreate this incident.

## Preserved state and ownership

The interrupted batch was `24e47c2864bd8fb4`, timestamp `2026-03-05T00:00:00Z`,
manifest SHA256
`74adea169fe1967d7c6128c1ff8e9eb3959b77025d4555e462c85375162e13b0`.
The checkpoint had 32 completed batches and an adjacent active intent for that
33rd batch. Its four manifest-bound input payloads survived. The old journal's last
batch event was `spark-commerce-payment-silver: started`; it also recorded completed
bronze. That journal tail does not establish the payment-silver commit outcome.

The original project had already undergone scoped cleanup with all six volumes
preserved. Before restarting its five core services, the operator confirmed that
no containers/writers for that exact owned project remained and that all six
expected volumes still existed. No replacement data, catalog reset, source reland,
image build/pull, unrelated service start or global prune was needed.

Original source `4902dc1aa8f9b0a1db51d45a2294b30844cde543` and recovery checkout
`cca72020cdfd480de6f7a7c0025973df848e0849` had identical Git objects for `src/`,
`jobs/`, `compose.yaml` and `compose.spark-memory.yaml`. Those object IDs are
recorded; newer evidence documentation did not change the runtime used in retry.
The existing memory overlay and development writer identity remained in use.

Before retry, the operator copied the checkpoint and active intent into a separate
recovery output and copied the SQLite journal through SQLite's backup API. The
original state/report files were retained as incident evidence. Only one copy was
used for running work; this does not permit concurrent runners with separate local
locks against the same warehouse.

## Observed retry and independent checks

The unchanged `run-commerce-batch` command rediscovered the pending manifest and ran
all nine stages. It did not skip stages using the old journal as a resume cursor.
Runner total was 107.649 s with a 900-second whole-run budget and existing
300-second stage budgets. That single duration includes process/JVM/SQL/IO and is
not an optimization or capacity comparison.

| Observation | Before retry | After retry |
|---|---:|---:|
| Checkpointed batches | 32 | 33 |
| Customer history rows | 320000 | 330000 |
| Current customers | 10000 | 10000 |
| SCD2 snapshots | 32 | 33 |

Independent checks read all 33 original fixture manifests and payload checksums,
all 10000 customer IDs/names in every fixture and the recorded literal SQL results.
Before retry, every customer had the expected 32 daily periods beginning February
1, 2026; afterward each had 33, with the final current period beginning March 5.
Every period had exactly 10000 rows, distinct customers and matching synthetic names.
The selected fixture independently implied 100 orders and 277225 captured revenue
cents; detail, batch-daily and current SQL results agreed.

The 32 prior checkpoint entries were byte-equivalent as parsed entries, and exactly
the selected batch's original manifest hash was added. The copied active intent was
removed. All old journal rows remained intact in the recovered journal; a new run
recorded readiness. Every table's previously observed snapshot IDs remained present.
Some tables can acquire metadata-only snapshots on replay: equal snapshot counts
across all tables are not an idempotency requirement.

A final idle attempt completed with no stages. Its current file inventory and full
snapshot sets were unchanged; observed idle child wall time was 1.213 s. Original
incident reports/checkpoint/intent/journal hashes still matched their pre-retry
values. The owned stack was stopped afterward, its network removed and all six
volumes retained. Foreign container IDs were unchanged across this recovery
operation; that does not assert unchanged foreign state across the earlier outage.

## Operator procedure and remaining boundary

1. Inspect the exact project's ownership, tasks and service logs. Confirm no writer
   is active. Preserve original manifests, checkpoint, intent, journal and failed
   reports before choosing a working state path. Do not clear an intent or fabricate
   completion to bypass the normal planner.
2. Restart only its required core services against the preserved catalog/warehouse
   volumes, without recreating missing data. Verify source/runtime compatibility and
   inspect the current warehouse. If original inputs or catalog are missing, stop;
   this observed retry does not establish a restore procedure.
3. Under exclusive writer ownership, use the existing runner with the preserved
   state, selected project's credentials and endpoints:

   ```text
   uv run --frozen lakeops run-commerce-batch --state <working-checkpoint.json> --s3-endpoint-url <owned-minio-url> --server <owned-trino-url> --s3-bucket lakehouse --user lakehouse-performance --attempts 3
   ```

4. Verify the expected selected batch/manifest, literal history/current/revenue
   oracles, unchanged prior checkpoint entries and a compute-free idle replay.
   Keep the successful working checkpoint, intent lifecycle and journal together
   as the authoritative continuation state for that warehouse. In this incident,
   `artifacts/interrupted-recovery/checkpoint.json` is that 33-batch working state;
   the untouched 32-batch incident state is archival evidence, not a second writer.
5. Stop only owned services when finished and retain the volume set. Record cleanup
   separately from data correctness. Before destructive maintenance, still require
   the [retention preconditions](commerce-retention.md#before-maintenance), dry-run
   and a separate verified backup/restore rehearsal. No snapshot expiration, file
   compaction, orphan removal or shortened correction window was exercised here.

Retry can finish a previously committed stage again. This evidence establishes the
observed logical result and preserved state; it does not provide distributed
exactly-once execution or identify an interrupted Iceberg transaction boundary.
