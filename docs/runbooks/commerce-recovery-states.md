# Commerce batch state and recovery boundaries

Use this map before diagnosing a failed batch or preparing an active Spark
interruption experiment. It describes the existing single-writer runner, not a new
recovery mechanism. A batch can have committed Iceberg output while its processing
checkpoint is absent. An unfinished journal attempt can coexist with a completed
checkpoint. Neither observation alone determines whether a Spark commit happened.

## Who publishes each state

Paths below are repository-relative. For the normal CLI, the checkpoint defaults to
`data/state/commerce-batches.json`; active intent is `<state>.active.json` and the
journal is `<state>.runs.sqlite3`. Keep the actual paths used by the interrupted run.

| State | Publisher and publication boundary | What retry reads or does | Evidence and limits |
|---|---|---|---|
| Landed manifest | [`CommerceS3LandingZone`](../../src/lakehouse_ops/ingestion/commerce_s3_landing.py) uploads verified table objects before `manifest.json`, the commit marker. Each object uses conditional `IfNoneMatch="*"`; an existing object must match. | [`CommerceBatchPlanner.discover`](../../src/lakehouse_ops/ingestion/commerce_batches.py) lists commit markers and validates metadata, manifest checksum, identity, timestamp and inventory. Bronze independently validates the selected manifest and table bytes. | [`test_commerce_s3_landing.py`](../../tests/test_commerce_s3_landing.py), `test_lands_verified_tables_before_manifest_and_retries_safely`; [`test_commerce_bronze.py`](../../tests/test_commerce_bronze.py). Manifest-last publication is not a multi-object transaction or permission to mutate a committed batch. |
| Active intent | Planner `begin` revalidates selection, then writes the chosen batch identity with `_write_state`: temporary file, flush/fsync, `os.replace`. This precedes compute. | Planner prioritizes an unfinished active batch even if an older event-time batch arrives. Changed/missing manifests or corrupt intent fail closed. Intent for an already checkpointed batch is ignored after identity validation. | [`test_commerce_batches.py`](../../tests/test_commerce_batches.py), `test_active_batch_blocks_unrelated_commit_and_detects_changed_manifest`, `test_completed_checkpoint_with_leftover_intent_does_not_replay_compute`; [`test_commerce_late_queue.py`](../../tests/test_commerce_late_queue.py). Intent selects work; it does not fence a remote writer or acknowledge completion. |
| Attempt and container journal | [`CommerceRunJournal`](../../src/lakehouse_ops/commerce_run_journal.py) appends each event in a separate SQLite transaction with `synchronous=FULL`. A new journal instance has a new run ID. Container launch intent precedes Docker invocation. | Retry creates a new attempt. Planning does not resume from the last completed journal stage: the selected batch runs through the stage sequence again. The journal supports diagnosis and ownership inspection. | [`test_commerce_run_journal.py`](../../tests/test_commerce_run_journal.py), `test_real_process_exit_retains_incomplete_attempt_and_retry_appends`; [`test_commerce_container_journal.py`](../../tests/test_commerce_container_journal.py). A diagnostic event is not a warehouse or checkpoint transaction. |
| Customer SCD2 | [`apply_customer_history`](../../jobs/spark/commerce_customer_scd2.py) closes old current versions and inserts new versions in one Iceberg MERGE. Rebuild computes history from retained customer silver and publishes a single full-table overwrite when content differs. | Forward replay compares attribute hashes; a late declared `full_snapshot` selects rebuild. Rebuild rejects input that loses existing customers or their earliest known version and validates published content. | [`check_commerce_scd2_atomic.py`](../../tests/integration/check_commerce_scd2_atomic.py), [`check_commerce_customer_scd2.py`](../../tests/integration/check_commerce_customer_scd2.py), [`commerce-history-correction-demo.md`](commerce-history-correction-demo.md). These cover publication/replay boundaries; post-publication injection does not prove interruption inside commit. Complete retained silver and one writer remain assumptions. |
| Batch gold | [`gold_commerce_daily.write_selected_batch`](../../jobs/spark/gold_commerce_daily.py) merges missing rows by `(source_batch_id, order_day)` and compares existing/published content against selected silver. | Rerunning the same batch may repeat compute and add metadata snapshots; matching business rows are not appended twice. Conflicting existing rows refuse success. | [`check_commerce_daily_gold.py`](../../tests/integration/check_commerce_daily_gold.py), [`commerce-owner-exit.md`](commerce-owner-exit.md). This is a table publication boundary, not atomic publication of all commerce tables. |
| Current gold | [`gold_commerce_current.publish_snapshot`](../../jobs/spark/gold_commerce_current.py) creates or overwrites the complete current mart with source batch/time/manifest identity. Replay compares all rows; an older snapshot is ignored. | Explicit full-snapshot execution adds the current-gold stage. A late snapshot can correct customer history without rolling the current mart back to an older source time. Undeclared input does not enable this route. | [`test_commerce_snapshot.py`](../../tests/test_commerce_snapshot.py), [`commerce-history-correction-demo.md`](commerce-history-correction-demo.md). Snapshot identity unit tests alone do not exercise Spark. Absence in a full snapshot is not automatically a customer deletion event. |
| Processing checkpoint | [`_run_commerce_batch`](../../src/lakehouse_ops/commerce_pipeline.py) completes stages and the Trino batch-gold gate, then planner `commit` revalidates identity/order and atomically replaces the local checkpoint before removing active intent. | A matching processed batch is excluded from incremental planning. A matching existing commit returns `created=False`. Missing/changed committed manifests or corrupt state refuse progress. | [`test_commerce_batches.py`](../../tests/test_commerce_batches.py), `test_commit_is_atomic_idempotent_and_advances_incremental_plan`; [`test_commerce_checkpoint_exit.py`](../../tests/test_commerce_checkpoint_exit.py). Local atomic replacement is not host-power-loss testing, a cross-file transaction or distributed exactly-once. |

The production [gold gate](../../src/lakehouse_ops/commerce_gold_gate.py) checks
selected batch aggregate shape, nonempty orders and nonnegative revenue in
`commerce_daily`. It is not the complete literal business SQL oracle and does not
independently audit every SCD2/current-gold row. Keep stage postconditions and the
documented real-stack SQL acceptance alongside this gate.

## Failure windows and the next safe observation

Before retry, stop competing writers and use the checkout/retained storage that
owns the run. The normal CLI OS lock is local to the checkout. It does not cover a
different checkout, host, manually started Compose stage or external writer.

| Last observation | What remains uncertain | Next safe step |
|---|---|---|
| Table objects exist, manifest absent | Upload may be incomplete; no committed batch is discoverable. | Retry the same verified landing input. Do not manually publish a marker over unverified objects. |
| Active intent exists, no stage evidence | Compute may not have started, or the owner may have died before journaling. | Inspect the owner and recorded containers. Retry through the normal runner only once competing work has stopped; retain intent. |
| Stage `started`, no terminal event | The Docker client or owner may have died while Spark continued. The Iceberg commit outcome is unknown. | Inspect the exact recorded container and immutable ID using the [container inspector](commerce-container-inspection.md). Capture logs and table snapshots; journal silence is not proof of container absence or rollback. |
| Stage committed output, checkpoint absent | Publication and its journal completion are separate; earlier stages may also have committed. | Audit selected batch rows, SCD2/current identities and snapshots. Once writers are stopped, a normal verified retry reruns stages using the same immutable input. Never sum full-snapshot batches as independent revenue. |
| Gold gate failed or timed out | Spark output may be fully committed; query failure is not a failed write. | Retain manifest/state/history, inspect query and warehouse evidence, then retry through the runner. A query cancellation acknowledgement does not prove server resource reclamation. |
| Checkpoint replaced, intent or journal incomplete | Cleanup or a diagnostic append may have failed after acknowledgement. | Correlate batch ID and manifest with the [checkpoint inspector](commerce-checkpoint-exit.md). A normal incremental plan should exclude the processed batch. Do not delete its checkpoint to make the journal look complete. |
| Commit outcome unknown during an actual Spark interruption | Owner/container failure cannot locate the exact catalog commit boundary. | Inspect committed snapshots and business invariants before a verified retry. Preserve the distinction between observed container interruption and an injection inside Iceberg commit. |

The [explicit cleanup command](commerce-container-cleanup.md) can remove one
confirmed owned immutable container after operator acknowledgement. It does not
roll back a transaction or recover a batch. Never clear checkpoints, rewrite a
landed manifest, delete lock files or prune global Docker state to obtain a green
recovery run.

## Evidence available now and the next experiment

Local state/queue/journal tests use controlled S3 and stage substitutes unless their
description explicitly says otherwise. Real child-process tests establish process
exit, OS lock release and local persistence boundaries. The existing owner-exit
drill terminates the owner **after** the gold Docker invocation returns, while the
history correction drill injects failures after publication. Neither establishes
recovery from an actively interrupted Spark write.

For the next active-stage experiment, retain an isolated project and one immutable
manifest, then collect:

1. Source SHA, project, batch ID, manifest digest, state/journal paths, run ID and
   UTC timestamps. Record the selected stage and exact immutable container ID with
   matching run/batch/manifest/project/service labels.
2. Evidence that this stage is running Spark work before termination, not merely
   that a container exists. Capture Spark logs and before/after table snapshots.
3. The scoped termination result, runner exit/error, journal tail and unchanged
   checkpoint. Treat commit outcome as unknown unless snapshots establish it.
4. A fresh runner attempt against the same manifest, catalog and retained history.
   Check literal current/history/revenue oracles, no duplicate business rows or lost
   current versions, then checkpoint identity. Do not require equal snapshot counts.
5. A final idle replay that launches no compute. Retain failed attempts as evidence.

This document prepares that experiment; it does not report an active-stage test as
executed or establish durability across host/power failure.

## Recheck local state boundaries

```powershell
uv run --frozen pytest --no-cov -q tests/test_commerce_s3_landing.py tests/test_commerce_bronze.py tests/test_commerce_batches.py tests/test_commerce_late_queue.py tests/test_commerce_pipeline.py tests/test_commerce_run_journal.py tests/test_commerce_checkpoint_exit.py tests/test_commerce_container_journal.py tests/test_commerce_snapshot.py
```

This command starts no Spark/Trino stack and does not replace mandatory integration CI.
