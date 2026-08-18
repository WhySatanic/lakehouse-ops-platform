# Delivery roadmap

Each milestone is designed to be one reviewable pull request or a short sequence of
pull requests. Dates are intentionally absent: merge only completed, understood work.
The roadmap is open-ended; completing these phases establishes a stable core and creates
new expansion tracks rather than ending development.

## Phase 1 — reliable foundation

- [x] Repository standards, CI quality gate, architecture, and roadmap.
- [x] Open-Meteo client with response validation, bounded retries, atomic local landing,
  idempotency, and deterministic tests.
- [x] MinIO landing adapter with bucket bootstrap, object metadata, conditional writes,
  and tests against a pinned container.
- [x] Manifest-driven batch ingestion with bounded concurrency and partial-failure reports.
- [x] Storage readiness diagnostics for filesystem and S3-compatible landing backends.
- [x] Filesystem landing integrity audit with recovery-oriented failure reports.
- [x] PostgreSQL-backed Hive Metastore plus an automated schema/bootstrap check.
- [x] Spark + Iceberg writer producing bronze/silver tables from landed payloads.
  - [x] Idempotent bronze writer registered in Hive Metastore with S3 post-condition checks.
  - [x] Validated and deduplicated silver transformation with auditable rejects.
- [x] Trino coordinator/worker profile reading the same tables through Hive Metastore.

Exit criterion: one command starts the core stack and an end-to-end test lands a payload,
writes an Iceberg table with Spark, and validates it through Trino.

## Phase 2 — Iceberg operations

- [ ] Metadata collector for snapshots, files, manifests, and partition statistics.
- [ ] Rule-based table-health planner with explainable compaction decisions.
- [ ] Safe Spark executors for `rewrite_data_files`, `rewrite_manifests`,
  `expire_snapshots`, and orphan-file removal.
- [ ] Time-travel, rollback, schema evolution, and partition evolution scenarios.
- [ ] Failure injection for interrupted writes and post-condition reconciliation.

Exit criterion: maintenance produces a measured reduction in file count/query latency,
preserves declared snapshots, and emits an auditable execution report.

## Phase 3 — Trino performance and workload management

- [ ] Multi-worker topology with health checks and graceful shutdown runbook.
- [ ] Resource groups for ingestion, BI, and ad-hoc workloads with queueing tests.
- [ ] Repeatable query corpus and baseline capture (`EXPLAIN ANALYZE`, wall time, CPU,
  scanned bytes, peak memory).
- [ ] Partitioning, sorting, file-size, and metadata-cache experiments.
- [ ] Version-upgrade rehearsal with compatibility and rollback checks.

Exit criterion: a checked-in report explains a bottleneck, the chosen change, measured
improvement, trade-offs, and rollback path.

## Phase 4 — centralized access control

- [ ] Deny-by-default file policy and negative authorization tests.
- [ ] Ranger deployment and Trino plugin configuration.
- [ ] Versioned role-to-resource policy model and policy deployment automation.
- [ ] Row filters, column masking, audit delivery, and break-glass procedure.
- [ ] S3 service-account policies aligned with engine responsibilities.

Exit criterion: automated tests prove the role matrix, including denied access, masking,
row filtering, impersonation boundaries, and audit events.

## Phase 5 — observability and serving

- [ ] Prometheus collection, Grafana dashboards, and actionable alerts.
- [ ] Platform SLOs for query success, ingestion freshness, and maintenance backlog.
- [ ] ClickHouse serving profile and a documented Iceberg/S3 integration experiment.
- [ ] Recovery drill for lost worker, unavailable metastore, and restored metadata DB.

Exit criterion: a demo script and runbooks can diagnose and recover defined incidents.

## Sustainable contribution rhythm

A strong week contains one or two complete changes, not a fixed number of cosmetic
commits. A useful pull request includes an issue, acceptance criteria, implementation,
tests, documentation, and evidence. Suggested rhythm:

1. Design and acceptance criteria.
2. Implementation with focused commits.
3. Verification, measurements, documentation, and merge.
