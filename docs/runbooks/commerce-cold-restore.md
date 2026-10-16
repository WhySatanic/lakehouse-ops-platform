# Cold restore of retained commerce state

A local rehearsal restored the complete stopped fixture after its 33rd accepted
full snapshot into a separate Compose project. The restored catalog served
330,000 customer versions, 10,000 current customers and 100 orders with 277,225
captured revenue cents. Literal SQL checked every customer ID, revision name and
period boundary. All 15 tables' file counts, byte totals and retained snapshot sets matched
the source. The ordinary runner then returned idle without launching compute or
changing those inventories or its 33-entry checkpoint.

The [evidence](../evidence/commerce-cold-restore-2026-10-16.json) records query IDs,
SQL results, archive hashes, individual volume identities, state hashes and cleanup.
Raw archives/logs remain local operator evidence; the JSON is a derived report,
not a downloadable backup. This is one cold, single-writer fixture rehearsal.
It does not establish hot-backup consistency, a production recovery objective,
distributed exactly-once processing or completion of the separate 50-snapshot run.

## Establish the boundary

1. Resolve unfinished publication with the existing runner. The source here was
   the successfully recovered 33-batch warehouse, with no active intent. Its older
   interrupted 32-entry checkpoint is archival evidence and must not be reused
   as running state against that warehouse.
2. Record the exact Git source, Compose configuration, image digests, volume names,
   checkpoint and journal. Confirm exclusive ownership and stop only the owned
   project. Inspect **all** containers, including stopped containers, for users of
   each source volume. Refuse the rehearsal if ownership or quiescence is unclear.
3. Keep all source volumes and state unchanged. Create host backup directories
   outside the object-store prefixes that maintenance could delete. Copy the
   checkpoint and any active intent together; copy SQLite through its backup API.
   Keep an immutable backup of those state files separate from the restored
   working copy, whose journal can change even during an idle attempt.

This rehearsal used source `cca72020cdfd480de6f7a7c0025973df848e0849` and the
existing memory overlay. Its six source volumes were `bronze-input`,
`metastore-db-data`, `minio-data`, `trino-coordinator-data`, `trino-worker-data`
and `warehouse-data`. The last volume was empty; actual Iceberg objects were in
MinIO. Copying only the filesystem warehouse mount would have lost the data.

## Archive, restore and verify before startup

Use the same pinned PostgreSQL image as the catalog for its stopped physical
data copy. For this fixture the cached helper was:

```text
postgres:17.10-alpine3.23@sha256:8189a1f6e40904781fc9e2612687877791d21679866db58b1de996b31fc312e4
```

The following PowerShell command shape archives one confirmed owned volume.
Set `$volume`, `$archiveDir` and `$image` from the recorded fixture inventory;
repeat for all six while the complete source remains stopped.

```powershell
docker run --rm --pull never --network none --user 0:0 --entrypoint tar `
  --mount "type=volume,source=$volume,target=/data,readonly" `
  --mount "type=bind,source=$archiveDir,target=/backup" `
  $image -C /data -cpf /backup/volume.tar .
```

Give every archive a distinct recorded name. Create six **new** volumes for a
unique destination project with matching `com.docker.compose.project` and
`com.docker.compose.volume` labels. Refuse existing destination names. Restore
each archive to its matching new volume with a read-only archive bind and
`tar -C /data -xpf /backup/volume.tar .`. The source is never a restore target.

The first attempt was rejected before startup: this image's BusyBox tar restored
three Trino symbolic links with owner `0:0` instead of archived `1000:1000`.
The second attempt explicitly restored link ownership with `chown -h UID:GID
/data/LINK` from the trusted archive metadata. `-h` is essential: change the link
itself, without following its absolute target. Do not weaken the comparison to
ignore ownership differences.

Before starting services, archive each destination volume again. Independently
compare every member's relative name, type, mode, UID, GID, size and link target;
compare SHA256 of every regular file. Reject duplicate/unsafe paths and unexpected
file types. Raw tar hashes can differ due to header timestamps or enumeration
order, so archive-hash equality is not the member comparison. The successful
attempt matched all 7,779 members and 310,409,082 regular-file bytes across six
volume pairs. Preserve archive digests and both comparison inputs.

## Verify the running copy

Start only the five existing core services in the new project using fresh host
ports, `compose.yaml` plus `compose.spark-memory.yaml`, `--no-build --pull never`
and the same development writer identity. The existing MinIO initialization
dependency runs against the copied destination. No Spark stage or source reland
is needed for this verification. Preserved catalog S3 locations resolve to that
project's copied MinIO through the usual service names.

Compare the restored table file counts/byte totals and snapshot ID inventories
with the source evidence.
Run `customer_history_observation` for the fixture revisions `0..32` and all 10,000
customer IDs, then reconcile the selected batch's detail, daily and current gold
results. These existing helpers are in
[commerce_pipeline_profile.py](../../src/lakehouse_ops/commerce_pipeline_profile.py).
Run the ordinary `run-commerce-batch` against the **copied** 33-entry state and
the destination's MinIO/Trino ports. `measure_idle_replay` must report idle, no
stages, and unchanged files and snapshots. This observed idle took 2.343 seconds;
it is a single observation, not a performance comparison.

Finally collect destination logs and tear down only that project without volume
deletion. Verify its containers/network are gone, both complete volume sets and
archives remain, original state hashes are unchanged, and unrelated container
identities are unchanged. The successful attempt preserved all 14 unrelated
container IDs. The rejected first attempt's four partial destination volumes and
raw report also remain as evidence; they are not a usable restored warehouse.

This satisfies the restore rehearsal prerequisite for this particular retained
fixture. Continue with the [retention contract](commerce-retention.md) and a guarded
maintenance dry-run on freshly inspected state. It does not approve deletion or
shorten source/silver business-history retention.
