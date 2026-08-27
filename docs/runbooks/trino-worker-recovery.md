# Trino abrupt worker recovery

This drill proves a bounded recovery path when a worker disappears during an active
query. It is separate from the graceful shutdown drill because
it sends `SIGKILL`, gives the worker no drain window, and records the in-flight query's
terminal failure before retrying the data check on the surviving worker.

## Run the drill

Load the deterministic silver fixture and start the coordinator plus both workers. The
host process needs permission to inspect and control only the Compose-managed
worker container selected from the live task evidence.

```bash
mkdir -p artifacts
uv run python tests/integration/exercise_trino_worker_recovery.py \
  http://localhost:8080 artifacts/trino-worker-recovery.json
uv run python tests/integration/check_trino_worker_recovery.py \
  artifacts/trino-worker-recovery.json
```

Use the published Trino port if it differs from `8080`. For example, the local port
override `TRINO_HTTP_PORT=18080` requires `http://localhost:18080`.

## Failure injection and restoration

The runner performs these operations in order:

1. capture the silver row count, deterministic checksum, current Iceberg snapshot, and
   the three-node topology;
2. submit a CPU-bound aggregation over 100 million generated rows, identify the worker
   running its split, and map that node ID to its exact Compose service;
3. temporarily disable automatic restart for that exact worker and send `SIGKILL`;
4. require the in-flight query to reach `FAILED`, then require discovery to converge to
   one active worker;
5. repeat the fingerprint query on the surviving worker;
6. recreate only the selected worker in a `finally` path, restoring
   `restart: on-failure`;
7. require two active workers and the unchanged row count, checksum, and snapshot.

Disabling restart is part of the controlled injection. Without it, Docker may restart
the worker before Trino discovery exposes a stable degraded phase. Recreating the service
restores the versioned Compose policy and gives the restored container a new ID.

The evidence file uses schema version `1.0`. The validator rejects a missing target task,
a non-`SIGKILL` event, a query that did not fail, an incomplete degraded topology, data
drift, an unchanged container ID, or a restart policy that was not restored.

## Recovery contract

Trino in this profile explicitly uses `retry-policy=NONE` and does not claim transparent
fault-tolerant execution. Losing a worker during a task is therefore expected to fail the
in-flight query. Recovery means that a client can safely retry after scheduler convergence,
the remaining worker serves the same Iceberg state, and full worker capacity is restored.

The profile explicitly sets `query.remote-task.max-error-duration=15s` on every node.
Trino's default is one minute. The shorter local value keeps failure detection and CI
evidence bounded, but it is not a universal production recommendation. Operators should
choose a value that tolerates their measured network pauses without delaying incident
detection beyond the service objective.

This drill does not prove fault-tolerant execution with an exchange manager, concurrent
writes during worker loss, coordinator failover, or a metadata database restore. Those
remain separate capabilities.

## Upgrade from 0.39.0

No warehouse or metastore migration is required. The release adds a host-side recovery
runner, evidence validator, CI failure injection, and an explicit 15-second remote-task
error window. Recreate the coordinator and both workers so the new query-management
property is loaded.
