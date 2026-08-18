# Validated Iceberg silver pipeline

## Scope

The silver job reads the complete `lakehouse.bronze.weather_hourly` history and writes
two Iceberg tables:

- `lakehouse.silver.weather_hourly` contains one validated survivor for each
  `(location_name, observed_at)` key;
- `lakehouse.silver.weather_hourly_rejects` preserves invalid source rows with stable
  reject identifiers and one or more machine-readable quality errors.

For duplicate keys, the job keeps the row with the latest `ingested_at`; a checksum
descending tie-breaker makes the result deterministic. Replaying unchanged bronze data
does not add rows to either table.

## Quality contract

A row is rejected when identity or timestamps are missing, coordinates fall outside
geographic bounds, temperature is outside -100 to 70 degrees, relative humidity is
outside 0 to 100 percent, or precipitation/wind speed is missing or negative. Rejected
rows are not silently discarded and can be traced back through `object_checksum`.

## Run

Start the core services and build the shared Spark image as described in the bronze
runbook. Ensure bronze is current, then submit silver:

```bash
docker compose --env-file .env --profile catalog --profile compute run --rm spark-silver
```

The final JSON report includes bronze, valid, rejected, duplicate, silver, and reject
row counts. Repeat the command to verify idempotent behavior after a deployment change.

## Verify

Check registration and physical storage independently from the Spark exit code:

```bash
docker compose --env-file .env --profile catalog --profile compute run --rm silver-catalog-check
docker compose --env-file .env --profile compute run --rm silver-storage-check
```

The fixture-specific `silver-contract-check` is reserved for CI. It proves survivor
selection, uniqueness, rejection classification, and expected row counts against the
checked-in acceptance documents.

## Failure handling

1. If the bronze table is missing, run the bronze pipeline and its post-condition checks.
2. If every row is rejected, query `quality_errors` before changing any limits; do not
   delete the reject table to make the run appear healthy.
3. If a merge fails, preserve Spark and metastore logs and rerun only after confirming
   that the previous Iceberg commit either completed or remained uncommitted.
4. If catalog checks pass but object checks fail, stop downstream consumers and inspect
   MinIO availability and credentials before retrying.

Stop services without removing volumes during normal operation:

```bash
docker compose --profile catalog --profile compute down
```
