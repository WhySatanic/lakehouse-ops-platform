# Container image digest lock

## Scope

`config/images.lock.json` binds every external image used by:

- `compose.yaml` runtime and check services;
- the Hive Metastore and Spark Dockerfile `FROM` instructions;
- the source and target of the Trino upgrade rehearsal.

Local images under `lakehouse-ops/` remain source-built and are intentionally outside
the registry lock. Each external reference keeps its human-readable tag and appends the
reviewed multi-platform manifest digest.

## Verify coverage

Run the same check used by CI:

```bash
uv run lakeops verify-image-lock
```

A ready report records the lock digest, unique image count, total source uses, and source
categories. The command fails when an external reference is not digest-pinned, has no
lock entry, disagrees with the lock, or when the lock contains an unused entry.

Render all profiles after verification:

```bash
docker compose --profile catalog --profile compute --profile query \
  --profile security --profile observability --profile serving config --quiet
```

## Refresh one image

1. Read the upstream release notes and select an explicit tag.
2. Resolve its multi-platform manifest digest:

   ```bash
   docker buildx imagetools inspect IMAGE:TAG
   ```

3. Update the tag and digest together in `config/images.lock.json` and every reported
   source reference. Never update only the lock to make the verifier pass.
4. Run the image-lock verifier, unit tests, Compose rendering, and every integration
   profile that consumes the image.
5. Record behavior changes, migration steps, and rollback notes in the pull request.

For Trino, update both `config/trino/upgrade-rehearsal.json` endpoints and rerun the full
upgrade and rollback drill. The `TRINO_SERVER_IMAGE` override must use a locked tag and
digest from the reviewed plan.

## Rollback

Revert the source references and lock entry to the last reviewed tag and digest, then
rerun the affected integration profile. Cached images may keep a local recovery path
available during a registry outage, but a clean host still depends on registry
availability for its first pull.

## Trust boundary

Digest pinning prevents a mutable tag from silently changing fetched bytes. It does not
verify publisher signatures, generate an SBOM, scan vulnerabilities, or make an upstream
registry available. The lock stores manifest-list digests, so the registry still selects
the matching architecture-specific child manifest while preserving the reviewed image
set.
