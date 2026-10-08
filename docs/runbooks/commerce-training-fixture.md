# Commerce training fixture

This local source fixture supplies the related data that the weather example cannot:
customers, products, orders, and payments. It is intended for SQL joins, dimensional
modeling, incremental processing, late-arriving-data drills, and later gold marts.

Generate the default dataset:

```bash
uv run lakeops generate-commerce-fixture --output data/commerce
```

The command prints the generated batch directory.

## Related snapshot correction scenario

Generate a separate, small acceptance dataset:

```bash
uv run python -m lakehouse_ops.commerce_change_scenario --output data/commerce-changes
```

The JSON result lists three compatible version-1 batch directories and `expected.json`.
The source semantics are **full snapshots**, not additive change events. Batch 1 has
one order for 1,000 cents; batch 2 corrects the same business key to 1,500 cents and
changes the customer's name; batch 3 retains those values and adds a 700-cent order
whose event date is more than 30 days before arrival. Payments reference the same
order keys and reflect each snapshot's amounts.

The literal oracle records snapshot revenues of 1,000 / 1,500 / 2,200 cents, latest
daily revenue of 700 cents on January 1 and 1,500 cents on January 31, and two customer
history versions. Summing all three snapshots gives 4,700 cents and double-counts
the corrected order. An unchanged customer in batch 3 must not create a third version.

Rerunning verifies byte-identical content; conflicting output fails without replacement.
Use the existing `land-commerce-fixture` command for each reported batch path.
The real-MinIO smoke test publishes them in reverse order, verifies conditional replay,
and checks chronological planning, hash-bound commits and explicit batch replay.

CI also runs `tests/integration/check_commerce_change_scenario.py` with the existing
Spark `gold_commerce_daily.summarize` function in a bounded standalone container.
It checks all three batch revenues, the final daily values and one late order. To
repeat locally, generate the scenario under `artifacts/commerce-change-scenario`,
mount the repository read-only at `/repo` in the repository's Spark image, set
`PYTHONPATH=/repo/jobs/spark`, and submit that check with
`--root /repo/artifacts/commerce-change-scenario`. See the serving-integration CI
step for the full container command, including loopback host resolution for its
network-isolated Spark driver. This check uses Spark DataFrames without changing
the shared Iceberg catalog or Trino services.

This increment implements the acceptance fixture and S3 path. The current gold mart
remains batch-scoped. `expected.json` describes the intended latest-snapshot/history
semantics for subsequent Spark/Trino correction tests; it is not evidence that general
cross-batch correction or historical SCD2 backfill has already been implemented.

To inspect how source-fixture generation scales without starting Docker, use an
empty output directory:

```bash
uv run python -m lakehouse_ops.commerce_fixture_profile \
  --output-root artifacts/commerce-fixture-scale-data \
  > artifacts/commerce-fixture-scale.json
```

The probe generates 1,000- and 10,000-order fixtures with the same seed and
proportional quality cases, verifies each fixture by checksum replay, and records
rows, JSONL bytes, generation wall time, and peak traced Python allocations.
It refuses a nonempty output directory so cached fixtures cannot masquerade as
fresh timing. CI retains the JSON report as `commerce-fixture-scale` evidence.
Timing and Python heap depend on the runner and exclude native memory, Spark,
Iceberg, MinIO, and Trino. This is a source-generator growth probe, not an
end-to-end throughput or memory benchmark.

Land the default fixture's reported batch directory in MinIO after starting
`minio` and running `minio-init`:

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
Before any upload, landing verifies each local table has a positive row count matching
its manifest entry and a matching SHA-256 checksum. Planning also rejects a committed
manifest that declares zero rows for any required table, matching the bronze loader's
nonempty-table contract. If this fails, retain the fixture for inspection and regenerate
it from the intended configuration; do not edit the committed manifest in place.
The fixture, S3 landing, batch planner, and bronze loader all require an integer
`schema_version` of `1`. A missing, boolean, string, or newer version is rejected
before publication or compute. Preserve the incompatible manifest for inspection;
do not relabel it as version 1 to force a retry.
Landing rejects a fixture manifest larger than 1 MiB before uploading any table,
matching the planner's commit-marker read limit. It checks the size again before
publishing the marker. If rejected, retain the fixture for diagnosis and generate
a fresh bounded batch rather than editing an existing committed object.

Check that a committed source batch landed recently before running scheduled work:

```bash
uv run --env-file .env lakeops check-commerce-source-freshness \
  --s3-bucket lakehouse --max-age-seconds 900
```

The command checks the S3 modification time of the newest checksum-verified manifest,
not the fixture's `batch_at` event time or the age of a downstream gold table. It prints
JSON with the latest batch, commit time, age, and status. Exit code 0 means `ready`;
exit code 1 means `stale` because no committed batch exists or its age exceeds the
configured limit. An invalid manifest or missing S3 timestamp fails closed as an error.
This source check does not yet schedule or retry the downstream pipeline.

Check whether committed batches have waited too long for the local processing
checkpoint to advance:

```bash
uv run --env-file .env lakeops check-commerce-backlog \
  --s3-bucket lakehouse --state data/state/commerce-batches.json \
  --max-age-seconds 900
```

The JSON report names the oldest unprocessed commit and pending count. Exit code 1
means its S3 commit is older than the limit; exit code 0 means the backlog is within
the limit or empty. An empty backlog is not proof that new source data is arriving,
so run this alongside `check-commerce-source-freshness`. A missing or changed
processed manifest, unreadable checkpoint, or missing S3 timestamp is an error.
The check reads but never advances the checkpoint; only downstream success should
call `commit-commerce-batch`.

Send both freshness observations to the existing opt-in Alertmanager profile:

```bash
docker compose --profile observability up -d alert-webhook alertmanager
uv run --env-file .env lakeops notify-commerce-freshness \
  --s3-bucket lakehouse --state data/state/commerce-batches.json \
  --instance commerce-local --alertmanager-server http://localhost:9093 \
  --source-max-age-seconds 900 --backlog-max-age-seconds 900 \
  --alert-valid-seconds 180
```

`LakehouseCommerceSourceStale` identifies delayed landing; `LakehouseCommerceBacklogStale`
identifies delayed processing. Descriptions include the observation, batch identity,
next diagnostic action, and this runbook. Choose one stable, unique `--instance` for
each endpoint/bucket/prefix/checkpoint scope and keep it unchanged during recovery.
Batch IDs and ages are annotations, not identity labels. Alertmanager groups and
deduplicates repeated submissions; the bundled receiver retains webhook events.
The report's `notification: accepted` means HTTP acceptance, not confirmed receiver
delivery. CI separately verifies real firing and resolved webhook events for both alerts.

Exit code 1 means at least one freshness breach was submitted; 0 means both checks
were healthy and explicit resolved observations were submitted. Invalid manifests,
checkpoint failures, or notification transport/HTTP failures are errors (exit code 2),
not healthy reports. The command never writes the processing checkpoint. To recover
a real backlog, finish the Spark quality checks and verified gold completion first;
raising a threshold is only for the controlled CI drill, not incident remediation.

This is a one-shot direct [Alertmanager API client](https://prometheus.io/docs/alerting/0.34/alerts_api/),
not a scheduler. An external runner must invoke it regularly (for example every 60
seconds with the default 180-second validity), including after recovery. Validity is
bounded to 30..86400 seconds. Stopping submissions causes alerts to expire; a resolved
notification alone is therefore not proof of recovery. Retain the JSON observations
and monitor the runner's own liveness separately. Delivery uses a 10-second HTTP timeout
without automatic retries. An opt-in cron cycle is documented below; runner-liveness
monitoring remains planned. Existing check-only commands and notification configuration
are unchanged.

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
The rerun also requires the cached manifest to name exactly the four local JSONL
files with expected row counts, checksums, and quality-case metadata. A malformed
manifest, an outside-directory table path, or a symlinked manifest or table
exits 2 before hashing table files. The batch is left untouched. Preserve the
bad directory for diagnosis, generate
the same configuration under a separate empty output root, and reconcile its
batch ID and checksums before any manual recovery. Do not overwrite a committed
MinIO batch to hide local corruption.

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

The planner checks the commit marker's checksum and requires all four commerce
tables with matching JSONL filenames, positive row counts, and lowercase
SHA-256 values. A malformed marker stops planning without changing the
checkpoint. Inspect the retained MinIO object versions and regenerate the
fixture before retrying; planning does not read each table object. Planning
also stops if a paginated S3 listing repeats a continuation token instead of
selecting work from an incomplete inventory. Check MinIO listing health and
retry after it recovers; do not advance the checkpoint manually.
The planner reads at most 1 MiB plus one byte from each commit manifest and
rejects oversized objects before parsing or checkpoint changes. If this limit
is hit, retain the object for inspection and regenerate the bounded fixture;
do not trim an already committed manifest in place.

### One-shot batch runner

From the repository root, with Compose images built, MinIO initialized, Hive Metastore
healthy, and Trino coordinator/workers running, process one next pending batch:

```bash
uv run --env-file .env lakeops run-commerce-batch \
  --s3-bucket lakehouse --s3-endpoint-url http://localhost:9000 \
  --state data/state/commerce-batches.json \
  --server http://localhost:8080 --user lakehouse-ops
```

The runner selects exactly one committed batch in planner order, syncs landing input,
then executes bronze, payment/order/product/customer silver, customer SCD2, and daily
gold jobs in sequence. These jobs perform their built-in validation, reconciliation,
and post-condition checks. Only then does the selected-batch Trino gold gate allow the
checkpoint to advance. The selected manifest checksum is passed to bronze and checked
again before committing state, so changed source content cannot silently advance it.
The JSON result lists completed stages and verified gold totals; Spark/Compose logs go
to stderr. CI exercises a fresh fixture through this command, retains the report, and
then runs the existing exact-count/replay acceptance checks separately.

Successful runs also include `durations_seconds`: planner selection, each named Compose
stage, Trino gold verification, checkpoint commit, and the total. These are elapsed
monotonic-clock seconds, not CPU time. Verification includes retry waits and repeated
read-only queries. The total starts before planning and ends after the checkpoint; it
excludes CLI setup, S3 client creation, lock acquisition, and JSON printing. Idle and
failed runs do not claim completed-phase timings. CI checks the shape and nonnegative
values in the retained real-stack report. One run is diagnostic evidence, not a
volume benchmark, SLA, or per-job Spark memory measurement.

For transient coordinator or transport failures, opt into bounded read-only gold
verification retries with `--attempts 3 --retry-delay-seconds 2`. Defaults remain one
attempt and a two-second delay (unused without a retry). Attempts are bounded to 1..5
and delays to finite 0..60 seconds; invalid bounds fail before accessing S3 or starting
compute, even with an empty queue. Only transport errors and HTTP 429/502/503/504 are
retried. SQL, authentication, malformed responses, and failed gold quality checks fail
immediately. Compose/Spark stages never automatically repeat within this command;
exhausted verification leaves the checkpoint unchanged. These bounds limit attempts
and inter-attempt delay, not total query runtime or cleanup of abandoned read-only queries.
Retry warnings go to stderr, so stdout stays one JSON report. CI injects one HTTP 503 at
the Trino client boundary, then delegates to the real coordinator and retains separate
retry evidence alongside the pipeline result; it does not simulate a real server outage.

An empty queue returns `status: idle` without starting jobs or querying Trino. Stage,
gold, manifest, and checkpoint failures leave processing state unchanged; existing
Iceberg writes are not rolled back. Inspect the named failed stage before rerunning.
Earlier idempotent jobs may be replayed after a partial attempt, but an older changed
customer batch still fails SCD2 ordering checks. Keep a single state writer and immutable
commit markers.

The CLI acquires a nonblocking OS-backed lock at `data/state/commerce-runner.lock`
before accessing S3. It holds the lock through all jobs, Trino verification and checkpoint
completion, including idle checks. A competing runner in the same checkout exits 2,
identifies the held lock on stderr, and produces no stdout or work. This workspace-wide
lock also covers runners with different `--state` paths because they share input files.
Keep the lock file; do not delete or replace it during execution. The operating system
releases the lock when its owner exits, even abruptly, so no stale PID/lease cleanup is
needed. CI starts a competing CLI while the real pipeline owns the lock, verifies its
rejection, and retains that result in `commerce-runner-retry.json`.

This is a cooperative local-checkout lock, not distributed fencing. Separate checkouts,
manual jobs, low-level checkpoint commands and direct Python callers do not participate.
Always run from the same repository-root working directory, using one checkout and
local filesystem; another working directory selects another relative lock file.
Network-filesystem lock behavior
is not supported. Process exit does not stop already-running Docker/Spark children or
undo Iceberg writes. After an interrupted owner, confirm those jobs have stopped and
inspect partial work before rerunning. The lock alone is not crash-recovery proof.

This is an opt-in local Compose runner, not an in-process scheduler or a generic
remote-S3 runner.
Its S3 endpoint must refer to the same MinIO as Compose; Trino must query that stack.
Only the `landing` prefix is supported. The selected bucket overrides `LAKEHOUSE_BUCKET`
for child jobs. Services use `--no-deps` so they never bootstrap unrelated fixtures or
start missing infrastructure implicitly. No automatic Spark retry, per-job timeout,
cancellation cleanup, freshness policy, or notifications are added here. The external
cron path below is opt-in; measured recovery remains planned. Existing manual
commands remain supported.

### Opt-in scheduled cycle

For a host with a working Compose stack, Alertmanager profile, and cron, the following
one-shot wrapper runs at most one pending batch and then submits both freshness
observations. It invokes the existing CLI twice, so a pipeline failure does not skip
the notification attempt. A child-process launch failure is written to stderr with
the command name; the wrapper still attempts freshness notification after a runner
launch failure. A launch failure exits 2 unless an earlier nonzero pipeline exit
takes precedence. From the repository root, first run it manually:

```bash
uv run --env-file .env python -m lakehouse_ops.commerce_cycle \
  --s3-bucket lakehouse --s3-endpoint-url http://localhost:9000 \
  --state data/state/commerce-batches.json \
  --server http://localhost:8080 --user lakehouse-ops \
  --instance commerce-local --alertmanager-server http://localhost:9093 \
  --alert-valid-seconds 900
```

Before starting either child command, the wrapper validates the bounded Trino
verification retries and the Alertmanager URL, instance, freshness ages, and alert
validity. A typo in those options exits 2 without launching Spark or touching S3;
correct the configuration and rerun. This preflight does not prove service reachability.

To opt in to a ten-minute schedule, install this crontab entry after replacing the
checkout and log paths with absolute paths writable by the cron user:

```cron
*/10 * * * * cd /srv/lakehouse-ops-platform && uv run --env-file .env python -m lakehouse_ops.commerce_cycle --s3-bucket lakehouse --s3-endpoint-url http://localhost:9000 --state data/state/commerce-batches.json --server http://localhost:8080 --user lakehouse-ops --instance commerce-local --alertmanager-server http://localhost:9093 --alert-valid-seconds 900 >> /srv/lakehouse-ops-platform/commerce-cycle.log 2>&1
```

Run cron as the same account and checkout used for manual runs, with access to Docker,
the state file, and `.env`. Set alert validity longer than the schedule interval;
otherwise an alert can expire between submissions. A run may exceed ten minutes:
the runner's nonblocking lock rejects overlapping compute, and that cycle still
attempts freshness notification. Exit 0 means the pipeline succeeded or was idle and
both observations were healthy; exit 1 means a freshness breach; exit 2 means the
pipeline or notification failed. On dual failures the pipeline exit takes precedence;
inspect both command outputs in the log. Freshness alerts do not cover every pipeline
failure, so monitor cron execution, nonzero exits, and log retention separately.
This wrapper does not retry Spark jobs, guarantee schedule delivery, or provide
distributed fencing. Stop the crontab entry before maintenance or recovery drills.

Live-stack CI also starts this wrapper with a separate, empty checkpoint after the
fixture has landed. It retains the pending-run and idle-rerun JSONL, validates all
eight stages, the Trino gold result, checkpoint creation, and both accepted freshness
submissions against the same batch. The jobs replay the existing Iceberg rows; this
is scheduled-path acceptance evidence, not a volume benchmark or a cron timing test.

### Manual processing

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
verify the selected batch through a healthy Trino query profile. This general check is
not tied to the synthetic fixture's expected counts; it fails when the batch has no
queryable gold rows or any day has missing, zero, or negative order counts or negative
captured revenue:

```bash
uv run --env-file .env lakeops check-commerce-gold \
  --batch-id <batch-id> \
  --attempts 3 --retry-delay-seconds 2 \
  --server http://localhost:8080
```

Only after both checks succeed, advance the planner checkpoint:

```bash
uv run --env-file .env lakeops complete-commerce-batch \
  --s3-bucket lakehouse \
  --state data/state/commerce-batches.json \
  --batch-id <batch-id> \
  --server http://localhost:8080
```

This command repeats the batch-specific Trino gold gate and advances the checkpoint
only after it succeeds. Failed or empty gold results and query failures leave the
checkpoint untouched. Its JSON includes both verification and checkpoint results.
It does not run Spark or replace the per-model quality checks above. The lower-level
`commit-commerce-batch` remains available for callers that already verified downstream
success. Commits use a nonblocking OS lock at `<state>.lock`; a concurrent writer
gets a `checkpoint is busy` error without changing state. Wait for the active writer
and retry; the existing lock file is normal and must not be deleted as a recovery
step. This protects checkpoint writes, not the whole Spark pipeline. Keep one
pipeline runner per workspace and verify gold before any manual commit.

Completion retries only read-only Trino queries after transport failures or HTTP
429, 502, 503, or 504. SQL errors, malformed responses, and invalid gold results
fail immediately. The default is one attempt; opt in with `--attempts` (1 to 5)
and `--retry-delay-seconds` (0 to 60, default 2). Retry notices go to stderr;
stdout remains the final JSON report. Exhausting attempts never advances the
checkpoint. This is bounded query recovery, not pipeline scheduling or Spark retry.

The checkpoint is written atomically. A repeated commit is a no-op. Planning fails if a
processed manifest has changed, so the same batch ID cannot silently acquire different
content. A new commit must be for the earliest unprocessed source batch; attempting to
skip an older committed batch fails without changing the checkpoint. Process batches in
the order returned by `plan-commerce-batches`, especially before updating SCD2 history.
Table objects without a valid commit marker are ignored.

If the checkpoint contains invalid JSON, the planner reports it as unreadable. If it
contains valid JSON but has malformed metadata, planning, completion, backlog checks,
and the one-shot runner reject it without changing the file or starting compute. Each
processed entry needs a batch timestamp matching its committed MinIO manifest; a JSON
boolean is not a valid checkpoint schema version. Do not delete the checkpoint to bypass
processed history. Preserve a copy, restore a known-good checkpoint, and reconcile its
processed batch IDs, timestamps, and manifest checksums against committed MinIO markers
before retrying.

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
