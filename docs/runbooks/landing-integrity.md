# Landing integrity audit

## Purpose

Run the audit before replaying landed objects, restoring downstream tables, or
investigating unexpected source data. It detects malformed JSON, invalid weather
payloads, checksum drift, and disagreement between object metadata and the partitioned
filesystem path.

## Run

```bash
uv run lakeops audit-landing --output data/landing
```

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

Version 0.3.1 audits the filesystem landing adapter. S3 object listing, object versions,
and retention-policy validation are planned extensions.
