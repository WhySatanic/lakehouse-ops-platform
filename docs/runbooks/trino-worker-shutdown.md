# Trino worker graceful shutdown

The query profile runs a coordinator and two workers with independent node IDs, data
directories, and inherited image health checks. This drill drains `lakehouse-worker-2`
through Trino's management API, waits for discovery to remove it from scheduling, and
then reads the silver table and its snapshot metadata through the remaining worker.

Trino's [graceful shutdown procedure](https://trino.io/docs/current/admin/graceful-shutdown.html)
enters `SHUTTING_DOWN`, waits for the configured grace period, finishes active tasks,
waits once more, and exits. Writing worker state requires system-information permission.
The local profile therefore mounts `access-control.name=allow-all` on each worker, as
described by Trino's [system access control reference](https://trino.io/docs/current/security/built-in-system-access-control.html).
This is an explicit local-test boundary, not the planned centralized policy model.

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

## Operational boundary

Workers use `restart: on-failure`. An abnormal non-zero exit is restarted, while the
zero exit produced by a completed drain stays stopped. `shutdown.grace-period=5s` keeps
the local and CI drill bounded. Production values must exceed the longest expected task
duration and the surrounding orchestrator termination timeout must cover both grace
windows plus task completion.

The drill proves query continuity after scheduler convergence. It does not claim
fault-tolerant execution for an abrupt worker loss, preserve an in-flight synthetic long
query, or test authenticated TLS. Those are separate failure and security scenarios.

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
`on-failure`, mounts local worker system access control, and sets a five-second shutdown
grace period. Recreate both workers after pulling the release so the configuration and
restart policy take effect.
