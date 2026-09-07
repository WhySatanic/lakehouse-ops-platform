# Landing integrity audit

## Purpose

Run the audit before replaying landed objects, restoring downstream tables, or
investigating unexpected source data. It detects malformed JSON, invalid weather
payloads, checksum drift, and disagreement between object metadata and the partitioned
landing path.

## Run

```bash
uv run lakeops audit-landing --output data/landing
```

Audit an S3-compatible landing prefix with the same validation rules:

```bash
uv run lakeops audit-landing \
  --backend s3 \
  --s3-bucket lakehouse \
  --s3-prefix landing \
  --s3-endpoint-url http://localhost:9000
```

Credentials use the standard AWS environment variables. The S3 audit follows paginated
listings, reads every JSON object below the prefix, and verifies the `sha256` object
metadata written by the landing adapter.

The command emits one JSON report. A healthy non-empty landing zone exits with code 0.
An empty landing zone or any invalid object exits with code 1, so scheduled jobs can use
the command as a precondition.

## Respond to a failed audit

1. Preserve the failed object and the JSON report as incident evidence.
2. Do not edit the object in place or replay it into downstream tables.
3. Compare the object with its source or a known-good replica.
4. Re-ingest the affected location and date through the normal idempotent ingestion path.
5. Re-run the audit before resuming downstream work.

The SHA-256 checksum detects accidental corruption and inconsistent metadata. It is not
a signature or proof of origin: anyone able to replace an object can also calculate a
new checksum. Object-store access policy, versioning, and audit logs remain separate
security controls.

## Current scope

The command audits filesystem and S3-compatible landing adapters. Object-version
history and retention-policy validation remain outside the integrity report.
