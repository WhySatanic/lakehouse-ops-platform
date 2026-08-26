# Apache Ranger Admin readiness

The opt-in `security` profile runs the official Apache Ranger 2.9.0 images for Ranger
Admin, PostgreSQL, and Solr. This slice establishes a reproducible central policy service
without changing Trino authorization. The query profile continues to enforce the
generated deny-by-default file policy until policy bootstrap and live Ranger enforcement
have their own acceptance evidence.

## Start and verify

Copy `.env.example` to `.env` and start the three services:

```bash
docker compose --env-file .env --profile security up -d --wait --wait-timeout 420 \
  ranger-db ranger-solr ranger-admin
uv run --env-file .env python tests/integration/check_ranger_admin.py
```

Ranger Admin is available at `http://localhost:6080`. The readiness check authenticates
with the configured administrator account, reads Ranger's Trino service definition, and
requires the catalog, schema, table, column, user, query, and system-information resource
types plus the access types needed for selection, execution, impersonation, and operator
read/write access. A reachable login page alone does not pass the check.

The check prints a schema-versioned JSON report. It does not create a Ranger repository,
upload policies, enable Trino's Ranger plugin, or claim that Ranger is enforcing queries.

The `RANGER_ADMIN_USER` and `RANGER_ADMIN_PASSWORD` values configure only the readiness
client. On first boot Ranger creates its documented development administrator account.
Change that password in Ranger Admin, then put the matching client credentials in `.env`.
Set the database passwords before the first boot because an existing database volume keeps
the credentials with which it was initialized.

## State and recovery

Ranger policy and administrator state lives in `ranger-db-data`. Solr audit state lives in
`ranger-solr-data`. A normal stop preserves both volumes:

```bash
docker compose --env-file .env --profile security down
```

Use `down --volumes` only for a deliberate clean-room rebuild. Before removing a populated
database volume, export policies through the Ranger API or UI and record the image version.
If Admin is unhealthy, inspect `ranger-db`, `ranger-solr`, and `ranger-admin` logs in that
order because Ranger initialization depends on both backing services.

## Security boundary

The example credentials are public development defaults. The profile exposes Ranger Admin
over plain HTTP and has no external identity provider, TLS, or secret manager. Keep it on a
trusted local machine. A later increment must add Trino plugin configuration, bootstrap the
versioned role policy, prove positive and negative decisions through Ranger, and document
the authentication boundary before this becomes an enforcement path.

## Upgrade from 0.28.0

No data migration is required. The new services are isolated behind the `security` profile
and do not start with the catalog, compute, or query profiles. Existing Trino behavior is
unchanged. Pull the pinned images before the first run if deployment bandwidth is limited.
