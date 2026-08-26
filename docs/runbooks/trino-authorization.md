# Trino deny-by-default authorization

The query profile loads one generated file-based system access policy on the coordinator
and both workers. `config/access/role-policy.json` is the versioned source of truth for
roles, identity bindings, resources, and default permissions. The control plane compiles
it into `infra/trino/access-control-rules.json`. Rules are evaluated from top to bottom.
Known identities receive only their declared permissions; final rules deny unmatched
catalog, table, system-information, and session-property requests.

This is an authorization exercise, not authentication. The local HTTP profile trusts the
`X-Trino-User` header, so anyone who can reach port 8080 can claim another identity. Do
not expose this profile outside a trusted development machine. Both the file adapter and
Ranger enforce the matrix, while authenticated transport remains a separate increment.

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

For centralized enforcement, start Ranger, run `lakeops sync-ranger-policy`, set
`TRINO_ACCESS_CONTROL_PROPERTIES=./infra/trino/ranger-access-control.properties` and
`TRINO_AUTHORIZATION_MODE=ranger`, then recreate all Trino nodes. Pass `--mode ranger` to
the report validator. The [Ranger runbook](ranger-admin.md) contains the complete sequence
and rollback path.

## Change, validate, and deploy

Edit the role model, then render the Trino adapter:

```bash
uv run lakeops render-trino-access-policy \
  --model config/access/role-policy.json \
  --output infra/trino/access-control-rules.json
uv run lakeops render-trino-access-policy \
  --model config/access/role-policy.json \
  --output infra/trino/access-control-rules.json \
  --check
```

The first command replaces the artifact atomically. The second is read-only and exits
non-zero when the checked-in artifact has drifted from the model; CI runs this check on
every pull request. Review both the model and generated diff, then rerun the live
acceptance check. The cluster reloads the generated file every five seconds.

To roll back a bad rule change, restore the previous model and generated file together,
wait for the refresh interval, and repeat both checks. If a node reports a policy parse
error, restore the files and restart only the Trino services. Iceberg data and Hive
Metastore state are unaffected.

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

## Upgrade from 0.27.0

No data migration or service restart is required when the generated policy is unchanged.
Future access changes must be made in `config/access/role-policy.json`; direct edits to
the generated Trino JSON fail the CI drift check. Run the render command once after
pulling this release and review any generated diff before restarting Trino.
