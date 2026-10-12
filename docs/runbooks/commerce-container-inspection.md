# Observe journal-recorded commerce containers

This opt-in diagnostic connects durable stage ownership to the current Docker state.
It reads only recorded launch intents for an explicitly selected Compose project and
runs `docker container inspect` for those exact names. It never starts, stops or removes
a container, changes a checkpoint, writes the journal, or scans unrelated containers.

```powershell
uv run --frozen python -m lakehouse_ops.commerce_container_inspection `
  --journal artifacts/linked-ownership-20261012/run-journal.sqlite3 `
  --project lakehouse-ops-linked-ownership-20261012 --limit 100
```

The journal must contain `commerce_container_events` from the standard executor.
Legacy pipeline-only journals fail explicitly; they are not upgraded or repaired.
Default selection is the latest ten launch intents, maximum 100. `has_more=true`
means the report is truncated, not that remaining containers are absent. Projects
without recorded launches have an empty observation, not proof that nothing runs.

## Interpret observations

| Observation | Meaning |
| --- | --- |
| `running`, `exited`, `created`, `paused`, `restarting`, `dead`, `removing` | Docker state observed after matching the exact name and all five ownership labels. |
| `absent` | Docker returned an exact missing-container response for the recorded name. |
| `ownership_mismatch` | Name or run/batch/manifest/project/service label differs. Do not treat the existing object as the recorded stage. |
| `inspect_failed` | Docker unavailable, timed out, failed, or returned an unsupported response. This is not absence. |

`status=incomplete` and process exit 2 accompany any `inspect_failed` row. Other
observations produce exit 0, including ownership mismatches; this is a diagnostic
command, not a success, cleanup or release gate. Missing/unreadable SQLite journals
also exit 2. Each Docker request has a five-second limit; observations are sequential
and can span up to `5 * limit` seconds, not an atomic snapshot of the runtime.

Read-only run outcomes remain available through `commerce_journal_inspection`.
Container observations do **not** establish whether the runner process is alive,
whether a running stage is orphaned, or whether its Iceberg transaction committed.
Do not launch another writer or remove a recorded container solely because its
pipeline journal is unfinished. Existing OS locking, scoped timeout cleanup and
authoritative processing checkpoints remain unchanged. Automatic orphan recovery
and termination inside an active Spark transaction remain separate work.

## Fresh local evidence, 2026-10-12

The opt-in test uses one uniquely named, network-disabled Python container from the
existing Spark image, capped at 128 MiB. Real Docker observations distinguished
running, exited and absent states; a separate run identity produced an ownership
mismatch. An unavailable daemon produced an incomplete report, not an absent object.
The SQLite file remained byte-for-byte unchanged. Only the smoke test's own container
was stopped/removed in its fixture cleanup; the inspection function performs no cleanup.

```powershell
$env:LAKEOPS_RUN_CONTAINER_INSPECTION_SMOKE = '1'
uv run --frozen pytest tests/integration/test_commerce_container_observation.py --no-cov
```

Retained JUnit: `artifacts/container-inspection-docker-20261012-v2.xml`, SHA256
`a81aeff24d8f2bb8a5b8bc6338a9f8dddbc444e8287999e7d740d6b24895fa26`.
The driver also inspected the earlier completed linked scenario's real journal:
28 recorded containers were absent, with `has_more=false`.
Report: `artifacts/container-inspection-retained-20261012.json`, SHA256
`9dddf87a591912c2f0bacc2163eb16ae3e445ac0bb5a1c874e4d0f855d06bb29`.

This is a private Python-module diagnostic. No public CLI/JSON contract, version,
release, authorization policy or mandatory CI check changes.
