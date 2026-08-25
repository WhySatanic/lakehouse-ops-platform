# Trino deny-by-default authorization

The query profile loads one file-based system access policy on the coordinator and both
workers. Rules are evaluated from top to bottom. Known identities receive only their
declared catalog and table permissions; the final catalog, table, system-information,
and session-property rules deny unmatched requests.

This is an authorization exercise, not authentication. The local HTTP profile trusts the
`X-Trino-User` header, so anyone who can reach port 8080 can claim another identity. Do
not expose this profile outside a trusted development machine. Ranger integration and an
authenticated transport are separate roadmap increments.

## Implemented matrix

| Identity | Allowed | Denied by this slice |
|---|---|---|
| `platform_admin` | all Trino policy privileges | writes still meet the read-only Iceberg catalog boundary |
| `data_engineer` | read bronze, silver, and future gold tables | schema ownership and writes through Trino |
| `analytics_engineer` | read silver and future gold tables | bronze and schema ownership |
| `analyst` | read future gold tables | current bronze and silver tables |
| `service_ingest` | no Trino data access | all Trino catalogs |
| `lakehouse-operator` | read test data and system information, request worker shutdown | schema and table writes |
| unlisted identity | submit a query with no protected data access | every catalog and system information endpoint |

The `lakehouse-*`, `metadata-cache-readiness`, and `upgrade-*` names are bounded test and
operational identities used by existing acceptance drills. They have read-only access to
the fixture catalogs. They are not aliases for the product roles above.

## Run the acceptance check

Load the deterministic bronze and silver fixtures, then start all Trino nodes:

```bash
docker compose --env-file .env --profile query up -d --wait \
  trino-coordinator trino-worker trino-worker-2
mkdir -p artifacts
touch artifacts/trino-authorization-report.json
docker compose --env-file .env --profile query run --rm \
  trino-authorization-check
uv run python tests/integration/check_trino_authorization.py \
  artifacts/trino-authorization-report.json
```

The container check proves four allowed operations and six denied operations against a
live coordinator. A denied case passes only when Trino returns an `Access Denied` error;
SQL mistakes and unavailable services do not count as authorization evidence. The host
validator then checks the complete case set and the explicit authentication limitation.

The report uses schema version `1.0` and is written to
`artifacts/trino-authorization-report.json`.

## Change and rollback

Edit `infra/trino/access-control-rules.json` in rule priority order. The cluster reloads
the file every five seconds. Review the fallback rules after every change and rerun the
acceptance check before merge.

To roll back a bad rule change, restore the previous file, wait for the refresh interval,
and repeat the check. If a node reports a policy parse error, restore the file and restart
only the Trino services. Iceberg data and Hive Metastore state are unaffected.

## Upgrade from 0.26.0

No data migration is required. Recreate the coordinator and both workers so every node
mounts the shared access-control properties and JSON rules. Existing anonymous local
queries now need one of the documented identities and permissions.

The sort-order evidence report moves from schema `1.0` to `1.1` and replaces
`create_sql_sha256` with `sort_order_sha256`. This keeps the performance inspector on
read-only Iceberg metadata instead of granting the write-capable catalog access that
Trino requires for `SHOW CREATE TABLE`.

```bash
docker compose --env-file .env --profile query up -d --force-recreate \
  trino-coordinator trino-worker trino-worker-2
```
