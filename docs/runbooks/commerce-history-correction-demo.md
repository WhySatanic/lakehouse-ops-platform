# One-command late customer history demonstration

With local Docker and the already built reference images, from the checkout:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_history_correction_demo `
  --output artifacts/commerce-history-correction-demo
```

Use a new empty output directory. The private module starts a fresh uniquely named
`lakehouse-ops-linked-history-demo-<uuid>` reference stack with automatically selected
ports, runs the production ingestion and correction acceptance, and removes its
service and completed initializer containers with scoped Compose `down`.
No images are pulled or built; named volumes, journal,
checkpoint, tables and logs are retained. Parent environment and unrelated projects
are unchanged. Port selection does not reserve ports until Docker starts.

## Scenario

1. Land three full snapshots in MinIO using conditional, content-verified replay.
   Process a new order, a changed order, and a late order/payment correction through
   all production Spark stages, current gold and independent literal Trino oracles.
   Captured revenue must be 1000, 1500 and 2200 cents respectively.
2. Verify snapshot-stable current-mart replay and a compute-free idle run for those
   three processed batches.
3. Land a late-effective customer observation for February 1 noon. Process its
   bronze and customer silver input, then rebuild the retained customer history.
   The literal oracle requires Alice, Alice Corrected, then Alice Updated, with
   contiguous midnight/noon/February 2 boundaries. Revenue remains 2200 cents.
4. Repeat the history rebuild. Exactly one history snapshot may be added across
   correction and replay; current-revenue snapshots and processing checkpoint bytes
   must remain unchanged. Two separate maintenance runs retain ownership journals.
5. Resume the same pending full snapshot through the normal runner. It chooses
   retained-history rebuild and executes all source stages and batch gold. After
   batch gold commits, inject a caught exception before current gold and checkpoint.
   The failed attempt must leave checkpoint bytes unchanged, preserve current
   revenue 2200 and the corrected history, and expose the late batch's own one-order,
   1000-cent result. Retry the same pending batch through all nine normal stages.
   Before checkpoint, require unchanged canonical rows and physical file identities
   across all discovered commerce tables, plus literal SQL results and unchanged
   history/current snapshot counts. Empty batch-gold MERGE metadata snapshots may
   be added, but no snapshot history may be removed. The existing recovery audit
   supplies the same rule as the standalone recovery drill; stale-snapshot
   protection must keep the newer current publication.
   Only this complete retry advances checkpoint four, then require compute-free idle.
6. Land a new complete February 4 snapshot with the same two orders and current
   customer attributes. The normal runner must select forward history, complete
   all nine stages and publish this batch as current. Before checkpoint, independent
   SQL oracles require revenue 2200 cents, the same corrected three-period history,
   and no additional customer-history snapshot. Only then record checkpoint five
   and require another compute-free idle attempt.

Steps 3 and 4 are explicit customer-only maintenance, not a completed fourth batch.
They do not process order/payment/product silver and leave the late source
uncheckpointed. Step 5 then processes that same complete full snapshot through
the normal runner, including those tables, before recording its checkpoint. The
intentional failed attempt is not a completed ingestion and records no checkpoint.
Idle is checked both before landing the late source and after its full completion.
Step 6 covers continuation into a newly landed ordinary batch, not merely draining
the earlier late batch. Its source is a new snapshot identity with unchanged state;
it does not add a fourth customer version or claim a new revenue change.

## Evidence and limits

`demo.json` records command outcomes and elapsed times. `acceptance/linked.json`
contains three batch oracles plus `late_customer_correction` with the corrected
history, replay status, checkpoint invariant and maintenance run IDs.
`late_batch_recovery` records the precise failure boundary, distinct failed/retry
run IDs, failed-attempt and retry wall seconds, before/after checkpoint SHA256,
snapshot counts before failure/after failure/before retry checkpoint, canonical
row and physical-file SHA256 inventories after failure/before retry checkpoint,
and literal batch/current/history results. The additional read-only inventory time
is outside the recorded failed attempt and inside the retry callback's wall time;
these are correctness-drill timings, not a controlled recovery benchmark.
Only the dedicated injected exception is caught for
the drill; unexpected stage, SQL, journal or checkpoint errors fail acceptance.
`acceptance/stages.log` and `acceptance/run-journal.sqlite3` retain actual stage
execution and ownership evidence. Failures retain logs and a failed report; only
this project's services are stopped, including after partially failed startup.

After the demo completes, independently recheck its retained evidence without
starting services or writing to the report, checkpoint or journal:

```powershell
$env:LAKEOPS_RUN_HISTORY_CORRECTION_SMOKE = '1'
$env:LAKEOPS_HISTORY_CORRECTION_DIRECTORY = 'artifacts/commerce-history-correction-demo/acceptance'
uv run --frozen pytest tests/integration/test_commerce_retained_history_correction.py -q -o addopts=
Remove-Item Env:LAKEOPS_RUN_HISTORY_CORRECTION_SMOKE
Remove-Item Env:LAKEOPS_HISTORY_CORRECTION_DIRECTORY
```

For older retained demonstrations, this read-only smoke requires six ready journal runs, exactly three processing
checkpoints, two separate maintenance runs without checkpoint events, five owned
maintenance containers and the literal corrected history. It also compares retained
file SHA256 values before/after inspection. It supplements, not replaces, the actual
Spark/MinIO/Trino execution.

Earlier continuation demonstrations additionally require a ready `late_batch_completion`, four
checkpoints, seven journal runs and `idle_after_correction`. The two maintenance
attempts still contain no checkpoint events. Historical evidence is accepted in
its original scope, not relabeled as proof of the new continuation behavior.

Earlier demonstrations with `next_batch_after_correction` additionally require five
checkpoints, eight ready journal runs and `idle_after_next_batch`. The next-batch
oracle must retain 2200 cents and the corrected history. Older four-checkpoint
reports keep their original scope and are not proof of this next-batch step.

New failure/retry demonstrations additionally require `late_batch_recovery`, exactly
nine journal attempts: eight ready and one specifically failed after batch gold,
with zero checkpoint events for the failure and one for its distinct retry. Both
attempts must reference the checkpoint's same late batch/manifest. The retained
checker verifies eight owned/completed stage containers for the interrupted attempt
and nine for the retry; it still requires only five completed batches, not six.
Checkpoint hashes must agree across the failure. Gold/history snapshot counts
must prove committed late gold and fixed current/history; row/file hashes must
prove unchanged published data across retry even if an empty MERGE adds metadata.
Historical reports without this
field retain their original scope and are not relabeled as failure/retry evidence.

Startup is bounded by 240 seconds, the acceptance child by 2400 seconds, and stop
by 120 seconds. Per-stage limits remain 300 seconds. Child timeout is not guaranteed
whole-process-tree cleanup; inspect the owned stage containers and logs afterward.

This bounded single-writer scenario requires complete retained silver history. It
does not demonstrate distributed exactly-once execution, interruption inside an
Iceberg commit, arbitrary source retention, general CDC/backfill planning, release
readiness or production capacity. Normal-runner correction selection is bounded to
explicitly declared complete snapshots and retained customer silver history.
It does not replace mandatory CI or release evidence.
The injected failure is a caught exception after a successful stage subprocess,
not process death, power loss, active-container interruption or an internal Iceberg
commit failure. The separate owner-exit and atomic-SCD2 tests retain their own,
narrower scopes.

The required `serving-integration` CI job invokes this same existing demo command,
then the retained-evidence smoke above. It stops its earlier disposable serving
profile first, so two lakehouse stacks do not compete for the runner's memory.
CI explicitly pulls the already pinned coordinator/worker images before invoking
the offline demo; catalog/Spark images were built earlier in that same job. Local
demo behavior remains no-pull/no-build, and no additional images are pulled locally.
The existing `serving-evidence` upload retains the ClickHouse report alongside the
demo's report, stage logs, checkpoint, ownership journal and retained-smoke JUnit,
including failed demo evidence. There is no new CI job or acceptance wrapper.
The job budget is 60 minutes, covering the preceding build/serving checks and the
existing 2400-second demo child, with startup/shutdown overhead. This wiring is not
by itself proof of a green mandatory CI run or a clean-checkout RC package.

## Compatibility

The existing linked and owner-exit demo commands keep their default behavior. The
new module selects the already implemented late-history acceptance flag explicitly;
it does not change public CLI contracts, table schemas, forward runner semantics or
version. The late-history opt-in path now deliberately adds eight stage launches for
the failed attempt and retains its failed journal entry. Its existing command and
timeouts are unchanged. Rollback removes this acceptance drill and restores the
previous late completion callback; no stored checkpoint, Iceberg data or schema reset
is needed. The default three-batch linked and owner-exit paths remain unchanged.

## Local execution, 2026-10-12

Fresh project `lakehouse-ops-linked-history-demo-8d9520866da54dc28100aca293c51209`
completed the production three-load path and both manual maintenance runs. Literal
revenue/history oracles passed: the final revenue was 2200 cents, corrected history
had three periods, exactly one history snapshot was added across correction/replay,
and checkpoint bytes and current-revenue snapshots stayed unchanged. The separate
read-only retained-evidence smoke also passed, confirming six ready journal runs,
three processing checkpoints and five distinct owned maintenance containers.

Retained report: `artifacts/history-correction-demo-20261012-v2/acceptance/linked.json`.
SHA256: `13fda16ba65bd13a8aad52d0ec489ebb4a3786f0266da9e2917855927a22f522`.
The parent demo report, logs and retained-smoke JUnit remain local evidence, not
release assets. Initial local quality passed all eight commands: 1065 tests passed,
19 expected opt-in skips, 92.20% coverage and three actual Docker smokes. Logs:
`artifacts/local-history-correction-demo-20261012-v1/`.

The first startup attempt is retained separately in
`artifacts/history-correction-demo-20261012-v1/`: Docker's default subnet pool was
exhausted before any service started. An already completed, owned test network was
verified empty and removed; no volumes or data were deleted. The second attempt
used a new project and output directory. After successful shutdown its own empty
network was also released to avoid consuming another subnet. Stopped containers
and volumes remain; reconnecting that retained project requires recreating its
Compose network rather than resetting data. No unrelated networks were changed.

After integrating current main including scoped cleanup, all eight local check
commands passed again: 1084 tests passed, 21 expected opt-in skips, 92.22% coverage,
and three actual Docker ownership/timeout/Spark-memory smokes. Evidence:
`artifacts/local-history-correction-demo-20261012-v2/`. Both real scoped cleanup
smokes and the read-only history evidence check also passed together, without
changing the retained history report/checkpoint/journal. JUnit:
`artifacts/history-correction-demo-20261012-v2-main211.xml`.

## Local next-batch execution, 2026-10-13

Fresh project `lakehouse-ops-linked-history-demo-4ca6649fe623486eae4c53874dcdd32d`
completed the three initial snapshots, both customer-only maintenance attempts,
full late-batch completion, and a newly landed ordinary February 4 snapshot.
That final batch completed all nine normal runner stages in 148.29 seconds,
retained revenue 2200 cents and the corrected three-period history without an
additional history snapshot, then reached compute-free idle. The retained smoke
passed with five checkpoints and eight ready journal runs; maintenance still
contained no checkpoint events. It did not modify report, checkpoint or journal.

Retained report: `artifacts/next-batch-demo-20261013-v2/acceptance/linked.json`.
SHA256: `53b5da8c1b103704a7addd37068aff8244d945345cd9aac2ba2fe25f57c6ea50`.
Read-only smoke JUnit: `artifacts/next-batch/retained-smoke.xml`.
Local quality passed all seven shared gates: 1180 tests passed, 23 opt-in skips,
92.22% coverage. Log: `artifacts/next-batch/quality.log`.
The complete acceptance child took 849.94 seconds. This two-order correctness
run is not a repeated performance benchmark or release-candidate evidence.

The first attempt retained in `artifacts/next-batch-demo-20261013-v1/` failed at
startup because Docker's subnet pool was exhausted. One empty network from the
previous completed, owned demonstration was inspected and released before retry.
The successful run stopped all five scoped core services; its own empty network
was subsequently released too. Stopped containers, volumes and evidence remain.
No unrelated container, network or stored data was removed.

## Local correction, failure, retry and next batch, 2026-10-14 UTC

Fresh project `lakehouse-ops-linked-history-demo-d542169ef8e043c4981c55723aad18ce`
completed the existing one-command path at source `eb343e22bf3f575e61c4da0ed374df001b5283b3`.
The subsequent rebase onto main `a8fe6bc` changed only the independently completed
profile documentation; the tested runner, jobs and acceptance code stayed identical.
The acceptance child took 865.59 seconds, excluding service startup and shutdown.

After the first three loads and customer-only maintenance, late full batch
`6ffe8636c50331a7` reached the dedicated caught failure after batch gold committed.
The failed attempt took 115.08 seconds and added no checkpoint. Checkpoint SHA256
before/after failure was
`22ee5d442338c7225d23edcd921d21f2ff05bbffc29136ba09df202f35dcf8a9`.
Its own gold was one order / 1000 captured cents, while current revenue remained
2200 and the corrected three-period history retained exactly one current customer.

The distinct normal-runner retry completed all nine stages in 125.57 seconds.
Before its checkpoint, all fifteen commerce tables retained the same canonical
row and physical-file hashes/counts. Batch-gold snapshots advanced 3 to 4 on
first publication, then to 5 on replay's empty MERGE metadata commit; current
and SCD2 stayed at 3. Empty metadata is not a duplicate data file or lost history.
Only the verified retry completed checkpoint four and reached compute-free idle.
The new ordinary batch `24c8c2e7c0479771` then completed all nine forward stages in
121.04 seconds, retained 2200 cents and the corrected history, published its own
current identity, completed checkpoint five and reached another compute-free idle.

The independent read-only retained smoke passed 16 tests, confirming exactly nine
journal attempts: eight ready and one dedicated failed attempt without checkpoint.
Interrupted/retry attempts owned eight/nine completed stage containers respectively;
the two customer-only maintenance attempts still had no checkpoint events.
Report/checkpoint/journal hashes were unchanged by inspection. Scoped shutdown
left zero project containers/networks and retained six named volumes. The three
running study SQL container IDs were unchanged; marketplace was not touched.

Evidence: `artifacts/linked-correction-retry-20261014-v2/acceptance/linked.json`,
SHA256 `0731cf8cd79d9f91b893bbd95053956b3f9c19d1de84657b2cbe3efb06d70591`.
Final checkpoint SHA256:
`9e062fb4d65576f4322cccc3ede22acd9b5c7bdef7f8acb74998675ae9a3515e`.
Read-only JUnit: `artifacts/linked-recovery-retained-v2.xml`.

The first attempt in `artifacts/linked-correction-retry-20261014-v1` is retained as
failed evidence: a new assertion wrongly prohibited empty batch-gold MERGE snapshots.
It failed before checkpoint, not through changed SQL results. The corrected drill
reuses the existing standalone recovery's row/file invariants and now records both
inventories before permitting checkpoint. Unit tests reject changed rows/files and
removed history while accepting metadata-only growth; the failed report is not relabeled.
This is one small correctness run with a caught stage-boundary exception, not repeated
large-load recovery timings, process death, a release or RC evidence.
