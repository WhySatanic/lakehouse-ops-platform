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
This increment intentionally stops at reproducible source generation. Spark ingestion,
Iceberg silver models, SCD2 customer history, and the gold daily mart are separate
executable increments rather than claimed capabilities here.
