# Commerce stage ownership evidence

The standard `run-commerce-batch` CLI now passes its existing execution journal to
each Docker stage. Before Docker is invoked, an append-only ownership row binds the
generated container name, run ID, selected batch ID, manifest SHA256, service, and
explicit Compose project hint. SQLite `synchronous=FULL` commits this `launch_intent`
before process launch. A journal-write failure prevents launch and therefore checkpoint.

The real container receives matching `lakehouse.ops.run_id`, `lakehouse.ops.batch_id`
and `lakehouse.ops.manifest_sha256` labels. Normal pipeline events and their phase remain
unchanged. This additive local evidence does not alter public CLI/JSON contracts.

Subsequent ownership events are:

- `completed`: Docker invocation returned success, not independently verified removal;
- `failed`: launch or checked process failure, not proof of absence;
- `timeout_removed`: the existing exact-name forced removal returned success;
- `cleanup_unconfirmed`: timeout cleanup failed or could not be confirmed.

`launch_intent` alone does not prove a container was created. A process killed before
or during Docker execution may leave only this row. If recording a terminal event
fails, the operation fails conservatively; its earlier intent and any written Iceberg
data remain. The checkpoint is still authoritative, not this ownership log.

## Inspect, do not automatically delete

The journal lives beside checkpoint state as `<state>.runs.sqlite3`. Query:

```sql
SELECT run_id, batch_id, manifest_sha256, service, container_name, project_hint, event
FROM commerce_container_events ORDER BY sequence;
```

Then inspect only an exact recorded name:

```powershell
docker inspect <exact-container-name>
```

Compare the three ownership labels and the actual `com.docker.compose.project` label.
An empty project hint means project naming was inferred by Compose, not resolved by
this journal. Names can be reused and labels are diagnostic metadata, not security
credentials. This change does not persist immutable Docker IDs or implement safe
orphan deletion, writer fencing, automatic restart, or active-commit recovery. Do not
delete a container merely because an intent lacks a terminal row.

Direct Python callers can supply `journal=` to `run_compose_stage`; omitting it preserves
the prior behavior. Existing opt-in recovery drivers are not silently rewired here.

## Upgrade and rollback

The first ownership write creates `commerce_container_events` beside the existing
`commerce_run_events` table. Existing journals need no destructive migration; old
events remain unchanged. Older code ignores the new table. Retain the SQLite file
and checkpoint when rolling back; do not reset either. Retention/pruning is unchanged.
No product version or tagged release change.

## Local verification

The opt-in Docker acceptance uses the locally built OSS Spark image with Python sleep,
no network and 128 MiB, not a running Spark job or production recovery test:

```powershell
$env:LAKEOPS_RUN_DOCKER_TIMEOUT_SMOKE = '1'
uv run --frozen pytest tests/integration/test_commerce_stage_timeout.py --no-cov `
  --junitxml artifacts/commerce-ownership.xml -q
Remove-Item Env:LAKEOPS_RUN_DOCKER_TIMEOUT_SMOKE
uv run --frozen python -m lakehouse_ops.local_quality
```

It inspects a live container's actual labels against prelaunch journal evidence,
then exercises two timed-out attempts, distinct owned names, confirmed removal and
retained ownership events. Only containers spawned by these checks are removed.

Local evidence on 2026-10-12: both real Docker checks passed; seven local quality
gates passed with 893 tests, ten opt-in skips, and 90.73% coverage. Retained JUnit:
`artifacts/commerce-ownership-20261012.xml`, SHA256
`230984a03f5f2248293e5c5711e8f3485fd61e50851ba560c4a68f7cd5cda582`.
The quality summary is retained in `artifacts/commerce-ownership-quality-20261012.log`.
This is adapter/ownership acceptance, not a fresh full Spark commerce capacity run.

The transient-Trino recovery drill forwards the CLI's optional journal unchanged
through its stage interceptor. Its local regression covers both `None` and a real
journal object. Omitting this forwarding causes a pre-stage `TypeError` in the live
CI path; it must not be worked around by disabling journaling or its acceptance.
After this correction, fresh local quality passed with 894 tests, ten opt-in skips,
and 90.73% coverage. Both real Docker ownership/timeout checks passed again in
13.03 seconds. Retained artifact `artifacts/commerce-ownership-fix-20261012.xml`
SHA-256: `0ce4cfc00afb4366793e892b4f44a4d8fad7c449ae1486930e1fce22440d5599`.
The full remote integration must still pass on the corrected PR head.
