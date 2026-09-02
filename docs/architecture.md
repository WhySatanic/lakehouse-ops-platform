# Architecture

## Product boundary

Lakehouse Ops Platform owns three concerns:

1. A reproducible local lakehouse used as the system under management.
2. A Python control plane that observes tables and workloads, creates maintenance
   plans, and records execution results.
3. Executable operational evidence: integration tests, dashboards, benchmark reports,
   policy tests, and runbooks.

It is not intended to become a generic workflow orchestrator or BI tool.

## Data flow

Open data is first stored without semantic changes in an immutable landing zone. Spark
then converts landing objects into Iceberg tables across three namespaces:

- `bronze`: source-shaped records plus ingestion metadata;
- `silver`: validated, typed, deduplicated observations;
- `gold`: stable analytical marts and SLO aggregates.

Spark performs write-heavy transformations and Iceberg maintenance. Trino is the
interactive SQL and federation layer. Both engines resolve the same Iceberg tables
through Hive Metastore and read the same S3-compatible object store.

## Control plane

The control plane follows a planner/executor split:

- collectors read Iceberg metadata tables, Trino query history, and platform metrics;
- planners produce immutable proposed actions with reasons and safety bounds;
- executors submit Spark maintenance procedures or controlled Trino statements;
- an audit store records inputs, decisions, outcomes, and durations;
- reconcilers verify post-conditions instead of trusting a successful process exit.

The split allows planner logic to be unit-tested without a running cluster and supports
a dry-run mode for every destructive maintenance operation.

The cross-profile release boundary is declared in
`config/release/readiness-contract.json`. CI validates each report close to its producer,
then a final control-plane command revalidates the downloaded bundle and binds its
digests, source revision, core snapshot, and recovery invariants into one attestation.
This makes release readiness a machine-readable contract instead of an inference from
independent green jobs.

External container inputs are separately bound by `config/images.lock.json`. CI verifies
that Compose runtimes, Dockerfile bases, and both Trino upgrade endpoints retain a tag
and the matching multi-platform manifest digest.

## Security model

Authentication and authorization are distinct. The local profile begins with explicit
test identities and a deny-by-default Trino file policy. The Ranger profile becomes the
central authorization source and exercises catalog, schema, table, and column rules,
row filters, column masking, and access audits.

The intended roles are:

| Role | Responsibility | Typical access |
|---|---|---|
| `platform_admin` | cluster and policy administration | full platform control |
| `metrics_reader` | machine observability identity | read system metrics, no catalog or table grants |
| `data_engineer` | pipelines and table maintenance | write bronze/silver, read all data layers |
| `analytics_engineer` | curated models | read silver, write gold |
| `analyst` | interactive analysis | read approved gold objects only |
| `service_ingest` | machine ingestion identity | write landing/bronze only |

Policy tests will assert both allowed and denied operations. A successful positive test
alone is insufficient evidence of correct authorization.

## Operational constraints

- The core laptop profile targets 16 GB RAM and starts without observability or Ranger.
- Optional Compose profiles add monitoring, security, and ClickHouse independently.
- CI uses deterministic fixtures and focused integration tests rather than the full stack.
- Retention and orphan-file actions require minimum safety windows and dry-run evidence.
- Every benchmark records dataset seed, row count, engine configuration, and query plan.
