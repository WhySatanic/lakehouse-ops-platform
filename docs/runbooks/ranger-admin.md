# Apache Ranger policy enforcement

The opt-in `security` profile runs the official Apache Ranger 2.9.0 images for Ranger
Admin, PostgreSQL, and Solr. The file policy remains Trino's default. Operators can opt in
to the Ranger adapter after synchronizing the versioned role model.

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

The check prints a schema-versioned JSON readiness report. Synchronize the central service,
users, and policies before enabling the plugin:

```bash
uv run --env-file .env lakeops sync-ranger-policy
uv run --env-file .env lakeops sync-ranger-policy
```

The second run should report no created, updated, or deleted policies. The synchronizer
removes only Ranger's exact generated bootstrap policies and policies carrying the
Lakehouse Ops managed description. It leaves unrelated operator-managed policies intact.

Enable Ranger on every Trino node and run the live evidence matrix:

```bash
TRINO_ACCESS_CONTROL_PROPERTIES=./infra/trino/ranger-access-control.properties \
TRINO_AUTHORIZATION_MODE=ranger \
docker compose --env-file .env --profile query up -d --wait \
  trino-coordinator trino-worker trino-worker-2
TRINO_ACCESS_CONTROL_PROPERTIES=./infra/trino/ranger-access-control.properties \
TRINO_AUTHORIZATION_MODE=ranger \
docker compose --env-file .env --profile query run --rm trino-authorization-check
uv run python tests/integration/check_trino_authorization.py \
  artifacts/trino-authorization-report.json --mode ranger
```

The Ranger plugin downloads only policy, role, user-store, and tag bundles without an
Admin session. Administrative endpoints still require authentication. Live decisions are
written to the `ranger_audits` Solr collection.

## Break-glass access

An incident responder can receive a temporary role binding from a reviewed JSON lease.
The lease requires a distinct approver, incident ticket, reason, UTC issue and expiry
timestamps, and a TTL no longer than one hour. Validate producers against
[`config/access/break-glass-lease.schema.json`](../../config/access/break-glass-lease.schema.json)
before submitting the lease:

```json
{
  "schema_version": "1.0",
  "grant_id": "BG-2026-0042",
  "user": "incident-responder",
  "role": "platform_admin",
  "approved_by": "incident-commander",
  "ticket": "INC-4242",
  "reason": "restore lakehouse query service",
  "issued_at": "2026-08-26T11:55:00Z",
  "expires_at": "2026-08-26T12:25:00Z"
}
```

Apply the reviewed lease through the normal policy synchronizer and retain its JSON report
with the incident evidence:

```bash
uv run --env-file .env lakeops sync-ranger-policy \
  --break-glass-lease approved-break-glass.json \
  > break-glass-grant-report.json
```

An active lease adds the named user to the requested existing role. A future lease is
reported as `not_yet_valid`; an expired lease is reported as `expired` and is not added to
the effective policy. Run the same command at or after expiry to reconcile Ranger and
remove the temporary binding. The user record may remain in Ranger, but it has no managed
Lakehouse Ops access after revocation.

Ranger does not schedule this reconciliation. Production automation must run the sync at
least as frequently as the required revocation window and alert on failures. The CI drill
proves an initially denied user becomes allowed with an active lease, then becomes denied
again after expired-lease reconciliation. It does not claim production authentication or
a highly available scheduler.

The official Ranger 2.9.0 Trino service definition used here does not advertise row-filter
or data-mask support. Those controls remain a compatibility investigation rather than an
implemented capability.

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
and Trino over plain HTTP and has no external identity provider, TLS, or secret manager.
Keep it on a trusted local machine. Ranger provides centralized authorization here, but the
local Trino endpoint still trusts the supplied user header. This is enforcement evidence,
not a production authentication boundary.

## Upgrade from 0.30.0

No Iceberg, metastore, Ranger database, or base role-model migration is required. Existing
sync commands keep their previous behavior. To use break-glass access, provide a reviewed
lease with `--break-glass-lease` and arrange reconciliation at or before its expiry. Roll
back by omitting the lease and running the synchronizer once; this removes its temporary
role binding.
