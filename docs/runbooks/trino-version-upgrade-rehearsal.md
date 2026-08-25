# Trino version-upgrade rehearsal

This drill proves the checked-in Trino 482 to 483 upgrade and rollback path against the
same Hive Metastore, MinIO warehouse, Iceberg snapshots, catalog configuration, and node
data directories. It is a full-cluster replacement in an isolated environment, not a
rolling upgrade.

The version pair and official release-notes URL are pinned in
`config/trino/upgrade-rehearsal.json`. The normal platform default remains Trino 483.
`TRINO_SERVER_IMAGE` exists only as an explicit server-image override for rehearsals and
emergency rollback.

## Acceptance contract

The schema `1.0` report contains four ordered phases:

1. `baseline` on Trino 482;
2. `upgraded` on Trino 483;
3. `rolled_back` on Trino 482;
4. `restored` on Trino 483.

Every phase must prove:

- one coordinator and two workers are active on the expected version;
- all three configured node IDs remain stable;
- the coordinator container reports a concrete image ID;
- bronze, silver, reject, deduplication, winner, and quality-reject results are exact;
- the latest snapshot IDs for all three Iceberg tables are unchanged;
- the silver data-file count, record count, and total size are unchanged;
- the combined data fingerprint matches all other phases.

The target restoration runs in a `finally` path when a failure occurs after rollback.
An upgrade result is not valid unless compatibility, rollback, and final restoration all
pass.

## Release-note review

Trino 483 changes the default Web UI and includes S3 authentication breaking changes for
IAM role and Web Identity configurations. This platform uses static development access
keys for MinIO and configures neither `s3.iam-role` nor the removed
`s3.use-web-identity-token-credentials-provider`, so those changes do not require a
catalog migration. The repository has no third-party Trino plugins, so the 483 SPI break
does not apply.

The Trino deployment guidance says node identity must remain stable through upgrades and
recommends keeping the data directory outside the installation directory. The Compose
profile mounts persistent `/data/trino` volumes and checked-in node IDs for that reason.

## Run

Create the normal bronze and silver fixture, then start the query cluster on the source
version:

```bash
TRINO_SERVER_IMAGE=trinodb/trino:482 \
  docker compose --profile query up -d --wait \
  trino-coordinator trino-worker trino-worker-2
docker compose --profile query run --rm trino-query-check
```

Run and validate the complete transition:

```bash
uv run python tests/integration/exercise_trino_upgrade_rehearsal.py \
  http://localhost:8080 \
  artifacts/trino-upgrade-rehearsal.json \
  config/trino/upgrade-rehearsal.json
uv run python tests/integration/check_trino_upgrade_rehearsal.py \
  artifacts/trino-upgrade-rehearsal.json \
  config/trino/upgrade-rehearsal.json
```

The script stops the workers before the coordinator, recreates all three server
containers, waits for exact-version membership, and leaves Trino 483 running. Do not run
it against a shared or production cluster.

## Manual rollback

If the automated drill itself cannot restore the target, use the same checked-in source
image and recreate all server nodes:

```bash
docker compose stop trino-worker trino-worker-2
docker compose stop trino-coordinator
TRINO_SERVER_IMAGE=trinodb/trino:482 \
  docker compose --profile query up -d --force-recreate \
  trino-coordinator trino-worker trino-worker-2
```

After diagnosis, repeat with `trinodb/trino:483` to return to the supported default.
Never remove the metastore, MinIO, or Trino data volumes during this procedure.

## Limits

- The fixture is small and read-only during each transition.
- The drill does not test draining long-running queries or mixed-version membership.
- Ranger and custom plugin compatibility are not covered.
- Only the adjacent 482 to 483 transition is approved by this evidence.
- Container tags are paired with recorded runtime image IDs, but registry digest pinning
  remains future supply-chain work.

See the official Trino 483
[release notes](https://trino.io/docs/current/release/release-483.html),
[deployment guidance](https://trino.io/docs/current/installation/deployment.html), and
[CLI compatibility guidance](https://trino.io/docs/current/client/cli.html).
