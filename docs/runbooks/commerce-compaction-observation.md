# Commerce diagnostic compaction observation

This private opt-in drill measures actual Iceberg data-file rewriting on a copy of
the commerce daily mart. It complements the generic maintenance fixture experiment
with real commerce rows and a byte-for-byte unchanged processing checkpoint.
It does not compact the production mart or establish production capacity.

`commerce_daily` is partitioned by source batch. Its two files after the two-volume
profile belong to different partitions, so their count alone is not a compaction
opportunity. The drill creates the separate **unpartitioned**
`lakehouse.gold.commerce_compaction_daily` from the same rows and deliberately writes
six small files. This is artificial fragmentation, not observed production growth.

## Run on a retained owned scale-demo catalog

Use the stopped catalog retained by the two-volume profile. Read its `demo.json`
for the exact project and four exposed ports; set those environment variables,
including `COMPOSE_PROJECT_NAME`. The project must match
`lakehouse-ops-scale-demo-` followed by 32 lowercase hexadecimal characters.
Do not use an unrelated or production catalog. From this checkout root:

```powershell
$env:COMPOSE_FILE = Join-Path (Get-Location) 'compose.yaml'
docker compose --project-name $env:COMPOSE_PROJECT_NAME --profile catalog --profile compute --profile query up -d --wait --no-build --pull never minio metastore-db hive-metastore trino-coordinator trino-worker
uv run --frozen python -m lakehouse_ops.commerce_compaction_observation --output artifacts/commerce-compaction --state artifacts/pipeline-profile-demo/acceptance/checkpoint.json --server "http://localhost:$env:TRINO_HTTP_PORT" --confirm-diagnostic-compaction
```

Source gold must contain 6..100000 rows. The diagnostic table must not exist; this
command never drops or overwrites an existing table. A repeat needs another fresh
owned scale catalog and a new empty output directory, not deletion of prior evidence.
The checkout runner lock prevents cooperating local commerce writers; it is not a
distributed lock against other catalog clients. Run with one catalog writer.

Planning and schema validation run on the host, as with the existing maintenance
workflow. The Spark image has Python 3.10 and runs only the compatible executor.
The existing planner supplies the immutable plan and snapshot; the executor performs
a dry run before explicitly approved apply. Only `rewrite_data_files` on the fixed
diagnostic table is eligible. No snapshot expiry or orphan deletion is performed.

Each Spark process has a 300-second parent timeout and each Trino query a 60-second
budget. A parent timeout is **not** proof that its Docker container stopped. Inspect
only this project's one-off `spark-maintenance` containers before retrying, retain
the failure logs and do not reset the checkpoint or overwrite the diagnostic table.
After a successful run, stop only this drill's infrastructure, without removing data:

```powershell
docker compose --project-name $env:COMPOSE_PROJECT_NAME --profile catalog --profile compute --profile query stop trino-worker trino-coordinator hive-metastore metastore-db minio
```

## Evidence and acceptance

The output contains `prepare.log`, `prepare.json`, `plan.json`, `compact.log`,
`compact.json` and `compaction.json`. Acceptance requires identical canonical full-row
SHA256 and row counts before/after, unchanged Trino order/revenue sums, fewer live
data files and a new diagnostic snapshot. Source rows, files, snapshot and checkpoint
bytes must remain unchanged. The final report retains partial evidence on failure;
post-commit verification failure is not rolled back or reported as success.

Local observation on 2026-10-12 used the retained 100/1000-order catalog:

| Measurement | Before | After |
| --- | ---: | ---: |
| Diagnostic rows | 62 | 62 |
| Live diagnostic data files | 6 | 1 |
| Live diagnostic data bytes | 24304 | 4638 |
| Orders / captured cents | 1100 / 3012053 | 1100 / 3012053 |
| Trino physical input bytes | 24304 | 4638 |
| Trino query CPU ms | 31 | 11 |
| Trino query elapsed ms | 86 | 110 |

Preparation took 8.178 seconds; planning plus Spark compaction took 9.676 seconds.
The source retained two files, 8505 bytes and snapshot `3869733067106323702`.
All four row digests were
`10f77ef00c15c668530f637c9ca556c618e7402414f8f38a00feb1d7fcfa24b9`.
The final retained report at `artifacts/commerce-compaction-20261012-v2/compaction.json`
has SHA256 `8edc9afbe8049ef9518e15e98e75a71547e7a1a86fd81681652836e28ecbb638`.
Artifacts are local ignored evidence, not checked-in files or portable release assets.

These are one warm-catalog query before and one after, without repeated medians or
randomized query order. Lower bytes and file count are established; faster queries
are not. No maintenance-memory profile, recovery during compaction, multi-volume
maintenance benchmark, production sizing or release-candidate readiness is claimed.
The first attempt failed on a host-only Python import before any table write; the
retained failure is `artifacts/commerce-compaction-20261012-v1`.

This private observation does not change the stable CLI/JSON contract or bump the
version. A release still requires the existing release policy and fresh mandatory CI.
