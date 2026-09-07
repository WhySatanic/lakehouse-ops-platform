# Malformed landing metadata

Run `uv run lakeops audit-landing --output data/landing` before using a recovered
filesystem landing zone. A non-string `ingestion.location.name` is reported as an
invalid object rather than terminating the entire audit. Other files are still
checked, and the original files remain unchanged.

Inspect the report's item paths and errors. Restore damaged objects from a known
source or backup and rerun the audit; do not rewrite checksums to hide corruption.
An unhealthy report is not permission to delete an object.

This compatible fix preserves the CLI and JSON report structure and needs no data
migration. Regression tests cover null, numeric, boolean, list, and object names
alongside a valid object. The filesystem audit does not inspect S3 objects.
