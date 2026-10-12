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
