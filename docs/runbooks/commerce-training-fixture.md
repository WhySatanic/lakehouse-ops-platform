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
docker compose --profile catalog --profile compute run --rm spark-commerce-payment-silver
docker compose --profile catalog --profile compute run --rm commerce-payment-silver-check
docker compose --profile catalog --profile compute run --rm spark-commerce-order-silver
docker compose --profile catalog --profile compute run --rm commerce-order-silver-check
docker compose --profile catalog --profile compute run --rm spark-commerce-product-silver
docker compose --profile catalog --profile compute run --rm commerce-product-silver-check
docker compose --profile catalog --profile compute run --rm spark-commerce-customer-silver
docker compose --profile catalog --profile compute run --rm commerce-customer-silver-check
docker compose --profile catalog --profile compute run --rm spark-commerce-customer-scd2
docker compose --profile catalog --profile compute run --rm spark-commerce-daily-gold
docker compose --profile catalog --profile compute run --rm commerce-daily-gold-check
```

The Spark job verifies the local copy against the committed manifest, checks required
values and exact source row counts, then merges all four files into
`lakehouse.bronze.commerce_customers`, `commerce_products`, `commerce_orders`, and
`commerce_payments`. Each raw row receives a source hash plus an occurrence number. That
keeps the intentionally repeated order rows while making an identical batch replay insert
zero rows. A partial four-table attempt can be rerun safely.

The payment silver job reads only the selected bronze batch. Positive, non-NULL amounts
with complete payment identity enter `lakehouse.silver.commerce_payments`; invalid rows
enter `lakehouse.silver.commerce_payment_rejects` with explicit quality errors. The job
reconciles valid plus rejected rows to bronze and uses stable merge keys, so replay changes
neither table's cardinality.

The order silver job keeps one deterministic survivor per order ID, validates arithmetic
and selected-batch customer/product references, and retains duplicate or invalid rows in
`lakehouse.silver.commerce_order_rejects`. Valid rows expose `is_late` when the event is
more than 30 days older than the batch timestamp. Reconciliation and stable merge keys
make the step safe to replay.

The product silver job enforces complete identifiers and descriptions plus a positive
unit price. One deterministic row per product ID enters `lakehouse.silver.commerce_products`;
invalid or duplicate rows remain queryable in `commerce_product_rejects`. Exact
reconciliation and stable merge keys make identical replay cardinality-neutral.

The customer silver job keeps intentional NULL emails as valid rows and exposes
`email_is_missing` for completeness measurement. Missing required values, malformed
non-NULL emails, and duplicate customer IDs remain queryable in
`commerce_customer_rejects`. Exact reconciliation and stable merge keys make identical
replay cardinality-neutral.

The SCD2 job reads one explicit customer silver batch and writes
`lakehouse.gold.dim_customers_scd2`. A tracked name, email, missing-email, or registration
timestamp change expires the previous current row at the batch timestamp and inserts a
deterministic new version. Unchanged customers produce no version. The job fails when a
changed batch is not newer than current history, and verifies one current row plus valid
effective periods after each run. Run batches in ascending `source_batch_at` order;
identical replay inserts zero rows.

The daily gold job reads one explicitly selected batch of validated orders, customers,
products, and payments and publishes `lakehouse.gold.commerce_daily`. Its grain is
`(source_batch_id, order_day)`; `order_day` is the UTC date of the order event, including
late orders. `order_count`, distinct `buyer_count`, and `ordered_amount_cents` describe
accepted orders. `captured_revenue_cents` sums valid captured payments only; it is not
gross order value. Captured, noncaptured, rejected, and missing-payment counts expose
payment quality. Multiple payment records per order are aggregated before joining, so
they cannot multiply order counts or ordered amount. Payment records that cannot be
attributed to a validated order, unreconciled silver tables, and orders referencing
unvalidated customers or products stop the job before any gold write. An identical replay
inserts zero rows; conflicting existing rows fail instead of silently changing history.
The mart is batch-scoped, not a cross-batch deduplicated business ledger. Query it through
Trino after the serving profile starts:

```sql
SELECT order_day, order_count, buyer_count, captured_revenue_cents,
       rejected_payment_count
FROM lakehouse.gold.commerce_daily
WHERE source_batch_id = '<batch-id>'
ORDER BY order_day;
```

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
content. A new commit must be for the earliest unprocessed source batch; attempting to
skip an older committed batch fails without changing the checkpoint. Process batches in
the order returned by `plan-commerce-batches`, especially before updating SCD2 history.
Table objects without a valid commit marker are ignored.

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
