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

- [x] Metadata collector for snapshots, files, manifests, and partition statistics.
- [x] Rule-based table-health planner with explainable compaction decisions.
- [x] Safe Spark executors for `rewrite_data_files`, `rewrite_manifests`,
  `expire_snapshots`, and orphan-file removal.
  - [x] Snapshot-guarded `rewrite_data_files` with dry-run and reconciliation evidence.
  - [x] Bounded `rewrite_manifests` with dry-run and reconciliation evidence.
  - [x] Exact-ID snapshot expiration with retained time-travel evidence.
  - [x] Orphan-file removal.
    - [x] Bounded, non-deleting orphan inventory with deterministic review evidence.
    - [x] Exact candidate-set approval, deletion, and post-deletion reconciliation.
- [x] Time-travel, rollback, schema evolution, and partition evolution scenarios.
  - [x] Exact-snapshot time travel and rollback with Spark and Trino evidence.
  - [x] Schema and partition evolution with compatibility evidence.
    - [x] Nullable-column add and field rename with historical-schema evidence.
    - [x] Partition-spec evolution with mixed-layout evidence.
- [x] Failure injection for interrupted writes and post-condition reconciliation.

Exit criterion: maintenance produces a measured reduction in file count/query latency,
preserves declared snapshots, and emits an auditable execution report.

## Phase 3 — Trino performance and workload management

- [x] Multi-worker topology with health checks and graceful shutdown runbook.
- [x] Resource groups for ingestion, BI, and ad-hoc workloads with queueing tests.
- [x] Repeatable query corpus and baseline capture (`EXPLAIN ANALYZE`, wall time, CPU,
  scanned bytes, peak memory).
- [x] Partitioning, sorting, file-size, and metadata-cache experiments.
  - [x] Data-file compaction experiment with snapshot-linked Trino medians.
  - [x] Partitioning and sorting experiments with pruning evidence.
    - [x] Day-partition A/B experiment with processed-row and physical-input evidence.
    - [x] Sort-order experiment with selective predicate evidence.
  - [x] Metadata-cache experiment with controlled cold and warm runs.
- [x] Version-upgrade rehearsal with compatibility and rollback checks.

Exit criterion: a checked-in report explains a bottleneck, the chosen change, measured
improvement, trade-offs, and rollback path.

## Phase 4 — centralized access control

- [x] Deny-by-default file policy and negative authorization tests.
- [x] Opt-in HTTPS transport with password authentication before Ranger authorization.
- [x] Ranger deployment and Trino plugin configuration.
  - [x] Opt-in Ranger Admin deployment with PostgreSQL, Solr, and Trino service-definition
    readiness evidence.
  - [x] Trino plugin configuration, policy bootstrap, and live enforcement evidence.
- [x] Versioned role-to-resource policy model and policy deployment automation.
- [x] Row filters, column masking, audit delivery, and break-glass procedure.
  - [x] Trino allow and deny decisions delivered to the Ranger Solr audit store.
  - [x] Row-filter and column-mask policies with live acceptance evidence.
  - [x] Reviewed break-glass grant, expiry, and revocation drill.
- [x] S3 service-account policies aligned with engine responsibilities.
  - [x] Idempotent MinIO identities and live allow/deny matrix for ingestion, Spark, and Trino.
  - [x] Run ingestion, Spark, and Trino containers with dedicated credentials instead of
    bootstrap root.
  - [x] Dedicated Hive Metastore warehouse policy with clean E2E evidence.

Exit criterion: automated tests prove the role matrix, including denied access, masking,
row filtering, impersonation boundaries, and audit events.

## Phase 5 — observability and serving

- [x] Prometheus collection, Grafana dashboards, and actionable alerts.
  - [x] Prometheus black-box readiness collection for MinIO, Hive Metastore, and Trino.
  - [x] Grafana dashboards for platform health and workload signals.
    - [x] Provisioned core readiness dashboard with a live datasource check.
    - [x] Workload, maintenance, and freshness dashboards.
      - [x] Trino workload dashboard backed by live OpenMetrics query counters.
      - [x] Maintenance and freshness dashboards backed by live Iceberg table metrics.
  - [x] Actionable core-target alert with tested firing and resolved delivery.
- [x] Platform SLOs for query success, ingestion freshness, and maintenance backlog.
- [x] ClickHouse serving profile and a documented Iceberg/S3 integration experiment.
- [x] Recovery drill for lost worker, unavailable metastore, and restored metadata DB.
  - [x] Hive Metastore service outage and recovery with cache-disabled Trino failure,
    preserved PostgreSQL container identity, and unchanged Iceberg snapshot evidence.
  - [x] Abrupt worker loss with observed in-flight task failure, degraded-cluster retry,
    unchanged Iceberg fingerprint, and restored worker capacity evidence.
  - [x] PostgreSQL metadata backup, loss injection, and restore with catalog reconciliation,
    verified backup contents, unchanged catalog manifest, and unchanged Iceberg snapshot.

Exit criterion: a demo script and runbooks can diagnose and recover defined incidents.

## Phase 6 — 1.0 release hardening

Acceptance is recorded in the [1.0.0 release manifest](releases/1.0.0.md).

- [x] Cross-profile release-readiness attestation with versioned evidence inventory,
  artifact digests, semantic validation, and shared Iceberg snapshot invariants.
- [x] Stable public control-plane CLI and JSON compatibility policy with executable
  backward-compatibility checks.
- [x] Clean-checkout release-candidate rehearsal that preserves the complete attestation
  and upgrade/rollback evidence for publication.

Exit criterion: a release candidate created from a clean checkout has a stable public
contract and one reproducible, retained evidence bundle covering the complete core path,
centralized authorization, observability, and recovery.

## Phase 7 — sustainable 1.x operations

- [x] Digest-pin every external Compose runtime, Dockerfile base, and Trino upgrade image
  with a versioned lock and executable coverage verification.
- [x] Run CI actions on Node 24 with reviewed commit pins and fail-closed artifact
  digest checks for cross-profile readiness and release-candidate evidence.
- [x] Audit paginated S3-compatible landing prefixes with payload, path, and object
  metadata checksum validation.
- [x] Enable MinIO bucket versioning idempotently and fail readiness checks when the
  recovery invariant is absent.
- [x] Audit retained S3 landing object versions and inventory delete markers through
  the scoped ingestion identity.
- [x] Freeze critical CLI option semantics and reject required/default/type/choice drift
  across every public control-plane command.
- [x] Introduce Draft 2020-12 JSON Schema validation for the control-plane verifier
  report as the migration path for all public reports.
- [x] Validate the image-lock supply-chain report against its versioned JSON Schema at
  runtime and expose an auditable schema override.
- [x] Validate the cross-profile release-readiness attestation against its versioned JSON
  Schema at runtime and expose an auditable schema override.
- [x] Validate the release-candidate bundle report against its versioned JSON Schema at
  runtime and expose an auditable schema override.
- [x] Validate Iceberg metadata health reports against a versioned JSON Schema at runtime
  without overloading the database `--schema` option.
- [x] Validate explainable Iceberg maintenance plans against a versioned JSON Schema at
  runtime before execution automation consumes them.
- [x] Validate Trino query baseline reports against a versioned JSON Schema at runtime
  before performance automation compares observations.
- [x] Validate Trino compaction experiment comparisons against a versioned JSON Schema
  after reconciling snapshots and maintenance execution evidence.
- [x] Validate Trino partition-pruning experiment reports against a versioned JSON Schema
  after proving identical results and reduced scan volume.
- [x] Validate Trino sort-order experiment reports against a versioned JSON Schema after
  proving identical results, declared sort order, and reduced scan volume.
- [x] Validate Ranger policy-synchronization reports against a versioned JSON Schema after
  service, user, managed-policy, and optional break-glass reconciliation.
- [x] Require every declared public output to provide a resolvable `schema_path` in the
  executable control-plane compatibility gate.
- [x] Validate every declared output schema against the Draft 2020-12 metaschema before
  accepting the control-plane contract.
- [x] Confine output schemas to portable relative paths inside the contract directory,
  including canonical resolution that rejects parent traversal.
- [x] Bind each declared output `schema_version` to a required matching `const` in its
  JSON Schema so contract and producer versions cannot drift independently.
- [x] Require every output schema to declare the Draft 2020-12 dialect and a unique,
  non-empty `$id` before the control-plane contract is accepted.
- [x] Keep public output schemas self-contained by rejecting external `$ref` and
  `$dynamicRef` values recursively throughout each schema definition.
- [x] Resolve every local public schema reference and reject dangling JSON Pointers or
  anchors before runtime validation consumes the schema.
- [x] Reject duplicate static or dynamic anchor names within a public schema resource so
  local reference resolution cannot silently select an ambiguous target.
- [x] Resolve nested schema resource IDs canonically and reject duplicates within each
  public schema document before a registry can silently overwrite a resource.
- [x] Bind every public output schema to a normalized SHA-256 digest in the executable
  contract so reviewed schema content cannot drift silently.
- [x] Refresh reviewed public schema digests through an atomic control-plane operation
  that immediately reruns the compatibility gate.
- [x] Validate the complete refreshed contract before replacement and preserve the original
  bytes when validation or atomic replacement fails.
- [x] Verify downloaded release-candidate report/archive pairs offline against the report
  schema, expected source revision, complete manifest membership, and member digests.
- [x] Sign release-candidate assets with GitHub OIDC and Sigstore, retain the provenance
  bundle, and enforce repository, workflow, source, issuer, and runner identity in CI.
- [x] Execute authenticated Ranger acceptance through two private Trino workers while
  excluding the secure coordinator from task scheduling.

Exit criterion: each supply-chain increment has an explicit trust boundary, a repeatable
refresh procedure, and a CI check that rejects silent source drift.

## Sustainable contribution rhythm

A strong week contains one or two complete changes, not a fixed number of cosmetic
commits. A useful pull request includes an issue, acceptance criteria, implementation,
tests, documentation, and evidence. Suggested rhythm:

1. Design and acceptance criteria.
2. Implementation with focused commits.
3. Verification, measurements, documentation, and merge.
