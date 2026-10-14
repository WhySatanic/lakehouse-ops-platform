# Opt-in customer history rebuild

The default `spark-commerce-customer-scd2` mode remains `forward`: one selected batch,
forward changes only. Its existing rejection of an older changed batch is preserved.
The opt-in `rebuild` mode reads **all retained validated silver customer observations**,
orders each customer's states by `source_batch_at`, collapses adjacent equal attributes,
and rebuilds half-open `[valid_from, valid_to)` periods. The latest period is current.
Returning to an earlier attribute state creates another version, not global deduplication.

This supports a late-arriving observation with an earlier effective timestamp, including
one between two existing versions. Effective time is still the batch timestamp: it does
not invent business-event time or implement revision ordering for conflicting observations
at the same timestamp. Missing customers in a snapshot are not deletion events.

## Run against a scoped reference stack

Stop competing commerce writers first. Confirm the selected Compose project, warehouse
and credentials belong to this experiment. Never apply this to unrelated containers.
Use the existing Spark identity and an already running MinIO/Hive Metastore stack.

```powershell
$env:COMMERCE_CUSTOMER_HISTORY_MODE = 'rebuild'
try {
  docker compose --profile compute run --rm --no-deps spark-commerce-customer-scd2
} finally {
  Remove-Item Env:COMMERCE_CUSTOMER_HISTORY_MODE
}
```

No `COMMERCE_BATCH_ID` is required in rebuild mode. The job does not advance the
processing checkpoint. For a manifest explicitly declaring `source_semantics:
full_snapshot`, the normal runner now selects rebuild when its effective timestamp
precedes an already checkpointed batch. It processes all four silver sources and
batch gold, then current gold (which ignores an older snapshot), verifies batch gold,
and only then checkpoints. A partial customer-only maintenance run remains unprocessed
until this full path completes. The next newer snapshot uses forward history again.
Manifests without that explicit declaration keep their existing forward-only path.
The runner atomically records a hash-bound active batch in
`<checkpoint>.active.json` before compute. This is retry intent, not a completed
load. If a late manifest arrives while the selected newer batch is running or
after its publication fails before checkpoint, the next run resumes the active
batch first. It repeats every required stage and verification, then checkpoints
and removes the intent. The late full snapshot is subsequently rebuilt against
the completed newer history; the following normal snapshot runs forward and reaches
idle. A crash after checkpoint but before intent cleanup does not re-run that
completed batch. Changed or missing active manifests fail closed.

Upgrade note: existing checkpoints remain valid and need no migration. Retry intent
is recorded only by the updated runner. For a failure left by an older runner
without intent, inspect its journal and published tables before attempting recovery;
do not invent completion state or delete the checkpoint. Keep the intent beside
its checkpoint when moving or restoring an unfinished run. Use the existing runner
lock and exclusive writer ownership; this does not recover still-running orphan
containers or coordinate independent checkpoints sharing a warehouse.

Queue order and late/full-snapshot classification compare UTC instants, including
mixed zero/nonzero fractional seconds. For example, `00:00:00Z` precedes
`00:00:00.500000Z`. Existing manifest hashes, plan timestamp strings and checkpoint
timestamps retain their original normalized format; no checkpoint migration is needed.
Run it only with complete retained silver history and a single writer. Empty input,
missing required values, duplicate effective timestamps or customer/batch identities,
missing existing customers
and input starting after their earliest published version are rejected before writes.
These checks cannot prove that intermediate observations have not been removed by
retention. Incomplete silver history is not a valid rebuild source.

Both forward updates and rebuilds publish one Iceberg commit. Forward updates use a
staged MERGE to close old periods and insert replacements atomically; the rebuild
publishes its cached candidate with one overwrite. Equal row multisets
produce `action: replay` without a new snapshot. Published rows and period invariants
are checked afterwards. Failure after publication can leave a committed snapshot;
inspect the table and retry with the same complete source, do not reset checkpoints.
Concurrent writers or catalog/commit failures require separate operational evidence;
this change does not claim distributed exactly-once recovery.

Upgrade note: the forward writer now rejects existing customers with no current
version, including tables left between the former close/append commits. Do not bypass
this guard or reset the runner checkpoint. Inspect the affected history and, with
complete retained silver observations and exclusive writer ownership, use the opt-in
rebuild above to restore a valid table before resuming forward ingestion. No schema,
table location or public report change is required.

## Acceptance

The existing `check_commerce_change_scenario.py` Spark/Iceberg acceptance now also
checks a literal three-period oracle: Alice from February 1 midnight to noon,
Alice Corrected until February 2, then Alice Updated as current. It verifies one
replacement snapshot, snapshot-stable replay, rejection without writes of ambiguous,
empty and truncated input, and a later return to Alice producing a fourth version.
The original revenue oracle and default forward-mode rejection remain tested.

The same acceptance injects failures before and immediately after the forward MERGE,
with a changed customer and a new customer in one batch. Before publication the old
snapshot remains intact; afterwards both customers have their complete current versions
in exactly one new snapshot. Retrying the committed batch changes neither rows nor
snapshot count. A customer with zero current versions is rejected, as is one with
multiple current versions. These are single-writer Iceberg publication boundaries,
not a claim of an atomic transaction across tables or the runner checkpoint.

The same acceptance runs in mandatory serving CI and can be run locally:

```powershell
uv run python -m lakehouse_ops.commerce_change_scenario --output artifacts/history-scenario
$repo = (Get-Location).Path
docker run --rm --name lakeops-history-check --memory 2g --cpus 2 --network none `
  --hostname lakeops-change --add-host lakeops-change:127.0.0.1 `
  -e SPARK_LOCAL_IP=127.0.0.1 -e PYTHONPATH=/repo/src:/repo/jobs/spark `
  --mount "type=bind,source=$repo,target=/repo,readonly" `
  lakehouse-ops/spark-iceberg:3.5.9-1.11.0 `
  /opt/spark/bin/spark-submit --master 'local[2]' `
  /repo/tests/integration/check_commerce_change_scenario.py --root /repo/artifacts/history-scenario
```

This bounded correction acceptance uses actual Spark and local Hadoop Iceberg.
Additionally, the production Compose entrypoint was run on 2026-10-12 against the
scoped `lakehouse-ops-recovery-v2` MinIO/Hive Metastore stack and returned `ready`,
`action: replay`, `source_rows: 10`, `history_rows: 10` on its retained silver data.
That verifies the new mode's catalog/storage wiring, not a late correction through
the entire S3 ingestion pipeline. No table schema migration or stable CLI change.
