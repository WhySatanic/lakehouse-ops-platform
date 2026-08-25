# Trino metadata-cache experiment

This experiment measures Iceberg metadata reads with the Trino coordinator memory cache
enabled and disabled. Both catalogs use the same Hive Metastore, MinIO warehouse, table,
credentials, and cache bounds. Their checked-in properties must differ only at
`iceberg.metadata-cache.enabled`.

Each of three cycles restarts the coordinator, waits for all three Trino nodes to become
active, then runs the same `$files` aggregate once cold and once warm through each catalog.
The enabled and disabled catalog order alternates between cycles to reduce systematic JVM,
HMS, and object-store warming bias.

## Evidence contract

The schema `1.0` report records:

- SHA-256 digests of both catalog configurations and the single allowed difference;
- coordinator identity and three active nodes after every reset;
- the exact Iceberg snapshot seen through both catalogs;
- identical file count, record count, and total-size results for every cold/warm query;
- query IDs and complete Trino metrics for all 12 measured runs;
- cold and warm medians for cache-enabled and cache-disabled catalogs;
- an enabled reduction, disabled control reduction, and net percentage-point observation.

Latency improvement is not a validity gate. Small local fixtures can report
`no_clear_benefit`; that is a valid experimental result when the control and correctness
evidence pass. This avoids turning normal timing noise into a false performance claim.

## Run

Start the catalog, query, and compute profiles and create the partition-pruning fixture:

```bash
docker compose --profile catalog --profile query --profile compute up -d --wait
touch artifacts/partition-pruning-fixture.json
chmod 0666 artifacts/partition-pruning-fixture.json
docker compose --profile catalog --profile compute run --rm \
  spark-partition-pruning-fixture
```

Run the controlled cycles and verify the report:

```bash
uv run python tests/integration/exercise_trino_metadata_cache.py \
  http://localhost:8080 \
  artifacts/trino-metadata-cache.json \
  infra/trino/catalog/lakehouse.properties \
  infra/trino/catalog/lakehouse-cache-disabled.properties
uv run python tests/integration/check_trino_metadata_cache.py \
  artifacts/trino-metadata-cache.json
```

The exercise restarts only `trino-coordinator`. Workers remain running and must rejoin
before measurements begin. Do not run this command against a shared or production cluster.

## Interpretation

`enabled_elapsed_time_reduction_percent` compares end-to-end cold and warm medians with
the cache on. `disabled_elapsed_time_reduction_percent` measures background warming
without that cache. Elapsed time is primary because it includes coordinator planning;
operator wall time remains in the full metrics but does not isolate metadata planning.
The net percentage points subtract the control effect. `benefit_observed` means the
enabled reduction is positive and exceeds the disabled reduction; otherwise the report
says `no_clear_benefit`.

Iceberg metadata files are immutable. New table states create new metadata files, so this
cache does not require invalidating an existing file after a normal commit. The experiment
records Trino 483 defaults: a one-hour TTL, 2% of coordinator heap, and a 15 MB per-file
limit. Trino rejects these tuning properties in a cache-disabled catalog because that
subsystem is absent, so version pinning makes the default bounds reproducible here.

## Rollback

Remove the disabled control catalog mounts and its properties file. Remove the explicit
cache properties from `lakehouse.properties` to return to Trino defaults. No Iceberg table,
snapshot, or object-store migration is required.

## Limits

- The workload scans one synthetic table's `$files` metadata on a local cluster.
- Three cycles test repeatability, not production capacity or cache sizing.
- Coordinator restart is disruptive and belongs only in an isolated experiment profile.
- The experiment does not enable local-disk file-system or Parquet-footer caches.

Configuration follows the official Trino 483
[Iceberg metadata caching](https://trino.io/docs/current/connector/iceberg.html#iceberg-metadata-caching)
contract. The immutable-file behavior follows the
[Iceberg reliability model](https://iceberg.apache.org/docs/latest/reliability/).
