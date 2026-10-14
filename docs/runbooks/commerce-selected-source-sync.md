# Selected commerce source synchronization

The standard commerce runner now mirrors only its hash-bound selected batch from
MinIO into the shared bronze input volume. Previously each batch mirrored the whole
landing prefix, including unrelated batches. Spark still reads the same batch path;
ingestion, stage ordering, journal ownership and checkpoint rules are unchanged.

The runner sets `COMMERCE_SYNC_SCOPE=selected` only in the `bronze-input-sync`
child environment. The script requires a 16-character lowercase hexadecimal batch
ID and keeps the existing `source=commerce/batch_id=<id>` directory layout. It
rejects malformed IDs or unknown scopes before contacting MinIO. An empty source
fails without reporting readiness. Existing cached batches are not deleted.

## Compatibility and upgrade

The Compose service mounts `jobs/ingestion/sync_bronze_input.sh` read-only. Keep
that file with the checkout when updating Compose; no image rebuild is required.
Manual synchronization defaults to `COMMERCE_SYNC_SCOPE=all`, even when
`COMMERCE_BATCH_ID` is inherited. This preserves weather and full-landing flows.
To intentionally mirror a selected batch manually, set both variables. To retain
the previous manual full mirror, leave the scope unset or set it to `all`.

## Local verification

With Docker running and the existing reference MinIO tools and Spark images:

```powershell
$env:LAKEOPS_RUN_SELECTED_SYNC_SMOKE = '1'
$env:LAKEOPS_SELECTED_SYNC_EVIDENCE = 'artifacts/selected-sync.json'
uv run --frozen pytest tests/integration/test_commerce_selected_source_sync.py -q -o addopts=
Remove-Item Env:LAKEOPS_RUN_SELECTED_SYNC_SMOKE
Remove-Item Env:LAKEOPS_SELECTED_SYNC_EVIDENCE
```

The opt-in test starts its own uniquely named MinIO project and lands three full
commerce snapshots. On fresh isolated volumes it verifies exact file names and
SHA256 contents for selected and legacy full synchronization, repeats each mode,
and rejects an invalid batch ID. The full mode deliberately retains a batch ID in
its environment to cover the compatibility case. It stops only its own MinIO;
volumes and optional diagnostic evidence remain available.

Run the unchanged full linked scenario separately:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_linked_demo `
  --output artifacts/linked-selected-sync-demo
```

This exercises three production Spark pipelines, independent Trino revenue/history
oracles, replay and idle behavior. Use an empty output directory each time.

This optimization bounds transferred source data, not total pipeline runtime or
memory. It is not a cross-job CI cache, automatic source cleanup or release evidence.

## Bounded manifest discovery

The existing commerce CLI, linked demo and recovery drivers now read committed
manifests in groups of at most eight using their low-level Boto3 S3 client. The client
is created before worker threads. Custom Python clients remain serial by default;
only explicitly thread-safe clients should opt into `manifest_read_workers`.
This follows the [Boto3 client sharing constraints](https://docs.aws.amazon.com/boto3/latest/guide/clients.html).
No new CLI option, service, image or state migration is required. Reverting the
constructor opt-in to one restores serial discovery without changing checkpoint data.

Discovery still lists and validates **every** manifest on every invocation, including
already processed batches. It preserves chronological selection, processed content
checks, the 1 MiB body limit, stream closure and checkpoint guards. It does not cache
ETags or skip old metadata. Only one bounded group is submitted at a time; a failed
group cannot enqueue the remaining history. A one-manifest queue avoids a thread pool.

### Same-data local comparison, 2026-10-14

One retained isolated MinIO volume contains genuine conditionally landed fixtures in
four separate pending queues, with 1/10/100/1000 manifests. Each fixture has 100 orders,
10 customers and 5 products. No checkpoint is fabricated and no data is regenerated.
Three paired repetitions per queue alternate serial/eight-worker order on one service
and shared client. Both modes select the same hash-bound oldest batch and perform
exactly N manifest GETs for planning, 2N for planning plus execution-option resolution.

| Pending manifests | Serial median, seconds | Eight-worker median, seconds |
|---:|---:|---:|
| 1 | 0.009152 | 0.009399 |
| 10 | 0.058048 | 0.045221 |
| 100 | 0.503673 | 0.382261 |
| 1000 | 4.754573 | 3.409553 |

These timings exclude fixture landing and memory tracing. At 1000 pending manifests
the measured median reduction is 28.3%, not a whole-pipeline speedup. A separate
tracemalloc-enabled comparison has median Python traced peaks of 126811/173018 bytes
at 10 manifests and 2933176/2833391 bytes at 1000 (serial/eight workers). Those are
parser/client heap observations, not process RSS, Spark or full-stack memory; the
instrumented timings are slower and are not mixed into the table.

Raw timing evidence: `artifacts/history-planning-ab-timing-20261014.json`, SHA256
`7a9bf9439e81a6ad1e197183ddcdff89476feb14c9084513cff9ce5892f0190f`.
Separate traced observations: `artifacts/history-planning-ab-20261014.json`, SHA256
`ae546a1158aae7e43a730e1cf08010b5f79923b83d617b2699a96b4ce76408b6`.
The recorded `source_sha` identifies base `5764863`; discovery used the then-uncommitted
bounded-read implementation in this change. These local measurement files are not
source attestations or clean-checkout RC assets. Three subsequent real CLI plans on
the 1000-manifest queue retained the same selection with no checkpoint creation.
This measures pending landing discovery with reused connections, not controlled
OS/image cold starts or growth of completed Iceberg table history. No Spark/SQL
workload or recovery-at-scale result is claimed by this experiment.
No mandatory CI check is removed. Existing CI time is also spent on alert delivery,
recovery and other integration scenarios; fewer mirrored files do not establish a
measured whole-CI speedup.

## Retained local evidence, 2026-10-12

Real MinIO verification passed on fresh volumes: selected synchronization copied
exactly 5 files, full synchronization copied 15, and both repeated modes preserved
all SHA256 contents. Evidence: `artifacts/selected-sync-20261012-v2.json`, SHA256
`093ab38dbf7f5b2b044fded41e98dc4dcaef839203a51e8b4fff533fdeda42a5`.

The fresh linked demo completed all three production pipelines using selected
synchronization. Independent revenue oracles returned 1000, 1500 and 2200 cents;
customer history matched, current-mart replay was snapshot-stable and the final
idle run selected no batch. Evidence:
`artifacts/linked-selected-sync-20261012-v1/acceptance/linked.json`, SHA256
`70a4394968ab545a4c49faec8315eb6230af613d87073c9b6541a5cd3cfee010`.

After integrating current main, the one-command local check passed all eight
commands: 1039 default tests passed, 19 opt-in tests skipped, coverage 91.68%, and
all three actual Docker ownership/timeout/Spark-memory smokes passed. Logs and
JUnit are retained in `artifacts/local-selected-sync-20261012-v2/`. These are local
verification artifacts, not shipped release assets or a capacity benchmark.
