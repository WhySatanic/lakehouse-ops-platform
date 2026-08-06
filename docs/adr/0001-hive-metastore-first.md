# ADR 0001: Use Hive Metastore as the first Iceberg catalog

- Status: accepted
- Date: 2026-08-06

## Context

Iceberg supports multiple catalog implementations. A REST catalog would simplify some
local setup, but the target operational skill set explicitly includes Hive Metastore.
The project also needs Spark and Trino to resolve the same tables and demonstrate
metastore backup, upgrade, and failure behavior.

## Decision

The first complete platform profile will use Hive Metastore backed by PostgreSQL. Spark
and Trino will share that catalog and store table data in MinIO. Catalog configuration
will be isolated behind component configuration so a later REST-catalog profile can be
added without changing domain pipelines.

## Consequences

- The project exercises schema initialization, database backup, service health, and
  metastore compatibility rather than hiding them.
- Local startup is more complex and needs explicit readiness checks.
- The control plane must not assume that every catalog exposes Hive-specific behavior.
- A later catalog comparison can measure operational trade-offs with the same workload.

