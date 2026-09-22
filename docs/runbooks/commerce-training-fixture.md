# Commerce training fixture

This local source fixture supplies the related data that the weather example cannot:
customers, products, orders, and payments. It is intended for SQL joins, dimensional
modeling, incremental processing, late-arriving-data drills, and later gold marts.

Generate the default dataset:

```bash
uv run lakeops generate-commerce-fixture --output data/commerce
```

The command prints the generated batch directory. Land that exact directory in MinIO
after starting `minio` and running `minio-init`:

```bash
uv run --env-file .env lakeops land-commerce-fixture \
  --fixture data/commerce/batch_id=<batch-id> \
  --s3-bucket lakehouse
```

The destination is
`s3://lakehouse/landing/source=commerce/batch_id=<batch-id>/`. Four JSONL table objects
are written before `manifest.json`. The manifest is the commit marker, so downstream
jobs must process only batch prefixes that contain it. A retry returns `"created": 0`
after downloading and verifying each existing object's checksum. A local checksum
mismatch or conflicting S3 object fails the command instead of silently replacing data.

The default batch contains 10,000 customers, 1,000 products, 100,000 canonical orders,
100,000 payments, and 1,000 repeated order rows. It also includes exact, documented
quality cases:

| Case | Default count | Expected downstream handling |
| --- | ---: | --- |
| Customer email is `NULL` | 1,000 | keep the customer; measure profile completeness |
| Repeated order row | 1,000 | deduplicate by `order_id` before aggregation |
| Order arrives over 30 days late | 1,000 | include it in a bounded backfill |
| Payment amount is `NULL` | 1,000 | quarantine it from paid-revenue measures |

Each run writes JSON Lines files and a `manifest.json` under a content-addressed
`batch_id=<id>` directory. The batch ID depends only on the generation configuration.
Running the same command again verifies every table checksum and reports
`"created": false`; it does not silently replace data. A changed seed or row count creates
a separate batch.

For a fast exercise, reduce all counts explicitly:

```bash
uv run lakeops generate-commerce-fixture \
  --output data/commerce \
  --customers 100 \
  --products 20 \
  --orders 1000 \
  --null-customer-emails 10 \
  --duplicate-orders 10 \
  --late-orders 10 \
  --invalid-payments 10
```

The fixture is synthetic, deterministic, and requires no external API or paid service.

## Plan incremental processing

List only batches whose checksum-verified `manifest.json` commit marker is present, then
select one unprocessed batch in source event-time order:

```bash
uv run --env-file .env lakeops plan-commerce-batches \
  --s3-bucket lakehouse \
  --state data/state/commerce-batches.json \
  --max-batches 1
```

Pass each returned `path` to the downstream Spark job. Advance the checkpoint only after
that job and its quality checks succeed:

```bash
$env:COMMERCE_BATCH_ID = "<batch-id>"
docker compose --profile compute run --rm bronze-input-sync
docker compose --profile catalog --profile compute run --rm spark-commerce-bronze
```

The Spark job verifies the local copy against the committed manifest, checks required
values and exact source row counts, then merges all four files into
`lakehouse.bronze.commerce_customers`, `commerce_products`, `commerce_orders`, and
`commerce_payments`. Each raw row receives a source hash plus an occurrence number. That
keeps the intentionally repeated order rows while making an identical batch replay insert
zero rows. A partial four-table attempt can be rerun safely.

After the Spark report returns `"status": "ready"` and its table post-conditions pass,
advance the planner checkpoint:

```bash
uv run --env-file .env lakeops commit-commerce-batch \
  --s3-bucket lakehouse \
  --state data/state/commerce-batches.json \
  --batch-id <batch-id>
```

The checkpoint is written atomically. A repeated commit is a no-op. Planning fails if a
processed manifest has changed, so the same batch ID cannot silently acquire different
content. Table objects without a valid commit marker are ignored.

For a deliberate retry or backfill, name every batch and keep the same explicit bound:

```bash
uv run --env-file .env lakeops plan-commerce-batches \
  --s3-bucket lakehouse \
  --state data/state/commerce-batches.json \
  --max-batches 1 \
  --replay-batch <batch-id>
```

Replay planning never changes the normal checkpoint. More replay IDs than
`--max-batches`, duplicate IDs, and IDs without a committed manifest are rejected.
Iceberg silver models, SCD2 customer history, and the gold daily mart remain separate
executable increments rather than claimed capabilities here.
