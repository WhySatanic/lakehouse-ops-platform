# Trino worker graceful shutdown

The query profile runs a coordinator and two workers with independent node IDs, data
directories, and inherited image health checks. This drill drains `lakehouse-worker-2`
through Trino's management API, waits for discovery to remove it from scheduling, and
then reads the silver table and its snapshot metadata through the remaining worker.

Trino's [graceful shutdown procedure](https://trino.io/docs/current/admin/graceful-shutdown.html)
enters `SHUTTING_DOWN`, waits for the configured grace period, finishes active tasks,
waits once more, and exits. Writing worker state requires system-information permission.
The shared file policy grants `lakehouse-operator` read and write access to system
information while its fallback rule denies every unmatched identity. The same policy is
mounted on the coordinator and workers as required by Trino's
[system access control reference](https://trino.io/docs/current/security/built-in-system-access-control.html).

## Run the drill

Load the deterministic bronze and silver fixtures first, then start all three nodes:

```bash
docker compose --env-file .env --profile query up -d --wait \
  trino-coordinator trino-worker trino-worker-2
mkdir -p artifacts
touch artifacts/trino-worker-shutdown-report.json
docker compose --env-file .env --profile query run --rm \
  trino-worker-shutdown-check
uv run python tests/integration/check_trino_worker_shutdown.py \
  artifacts/trino-worker-shutdown-report.json
```

The check fails unless all of these post-conditions hold:

- discovery initially reports three active nodes and two active workers;
- the target reports `ACTIVE`, then `SHUTTING_DOWN` after the authorized request;
- discovery converges to one active worker and no longer lists the target;
- the target HTTP endpoint stops after the two five-second grace windows;
- current silver rows and Iceberg snapshot metadata remain queryable.

The report uses schema version `1.0`. The host validator rejects missing, stale, or
inconsistent evidence rather than inferring success from a container exit code.

## Authenticated Ranger drill

The secure variant proves a stronger boundary: it submits a long-running query through
the HTTPS coordinator with a verified certificate and password authentication, observes
an active task on one of the two private workers, and requests graceful shutdown as the
`lakehouse-operator` identity. The exact in-flight query must finish successfully before
the worker exits. The surviving worker then serves the Iceberg fingerprint query and the
drained worker is recreated to restore three-node capacity.

After the Ranger and secure-query profiles are ready and the deterministic tables exist:

```bash
docker compose --profile security --profile catalog --profile secure-query \
  cp trino-secure-coordinator:/etc/trino/security/trino.crt \
  artifacts/trino-secure-ca.crt
uv run python tests/integration/exercise_trino_worker_shutdown.py \
  https://localhost:8443 artifacts/trino-authenticated-worker-shutdown.json \
  --mode authenticated-ranger --password "$TRINO_AUTH_PASSWORD" \
  --ca-cert artifacts/trino-secure-ca.crt
uv run python tests/integration/check_trino_worker_shutdown.py \
  artifacts/trino-authenticated-worker-shutdown.json
```

CI also correlates the report's `platform_admin` query identity and
`lakehouse-operator` control identity with allowed decisions retained in the Ranger Solr
audit export. The evidence therefore covers the authenticated coordinator boundary,
centralized authorization, task continuity, worker exit, degraded reads, data
fingerprints, and restored capacity. Worker-to-worker traffic remains on the Compose
internal HTTP network protected by Trino's shared internal secret; the externally
reachable coordinator is the TLS/password boundary.

## Operational boundary

Workers use `restart: on-failure`. An abnormal non-zero exit is restarted, while the
zero exit produced by a completed drain stays stopped. `shutdown.grace-period=5s` keeps
the local and CI drill bounded. Production values must exceed the longest expected task
duration and the surrounding orchestrator termination timeout must cover both grace
windows plus task completion.

The default drill proves query continuity after scheduler convergence. The authenticated
variant additionally proves zero interruption for one observed synthetic read query. It
does not claim fault-tolerant execution for abrupt loss, uninterrupted writes, or a
production grace-period value. Abrupt loss has a separate recovery drill whose expected
in-flight outcome is failure followed by a degraded-cluster retry.

## Restore capacity

Start the drained worker and confirm both workers are active again:

```bash
docker compose --env-file .env --profile query up -d trino-worker-2
docker compose --env-file .env exec trino-coordinator trino \
  --server http://localhost:8080 --execute \
  "SELECT node_id, coordinator, state FROM system.runtime.nodes ORDER BY node_id"
```

## Upgrade from 0.19.0

No warehouse or metastore migration is required. The query profile adds the
`trino-worker-2-data` volume, changes worker restart behavior from `unless-stopped` to
`on-failure`, and sets a five-second shutdown grace period. Later releases replace its
temporary worker-local access setting with the shared deny-by-default policy. Recreate
both workers after pulling the release so the configuration and restart policy take effect.
