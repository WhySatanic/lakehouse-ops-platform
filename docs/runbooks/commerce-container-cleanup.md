# Explicit single-container commerce cleanup

This private opt-in operator command stops/removes exactly one journal-recorded stage.
It acquires the normal commerce runner OS lock in the **same checkout**, selects the
explicit project/run/container launch intent, and checks the name plus all five
run/batch/manifest/project/service labels. It then removes the immutable Docker ID,
never the reusable container name, and independently confirms that ID is absent.

## Use

Run from the checkout root used by the interrupted runner. First stop competing
writers and inspect the exact recorded stage with the read-only
[container inspector](commerce-container-inspection.md). Confirm the journal, project,
attempt and stage are your experiment, and assess any active Spark/Iceberg write before
acknowledging termination. Substitute actual IDs; do not copy placeholder identities.

```powershell
uv run --frozen python -m lakehouse_ops.commerce_container_cleanup `
  --journal artifacts/interrupted/journal.sqlite3 `
  --project lakehouse-ops-your-experiment `
  --run-id YOUR_32_HEX_ATTEMPT_ID `
  --container lakeops-commerce-YOUR_32_HEX_STAGE_ID --confirm-stop
```

Without `--confirm-stop`, no Docker operation occurs. A held local runner lock prevents
even inspection. No recorded launch, ambiguous duplicate intents, ownership mismatch,
unsupported/removing state or malformed immutable ID refuses removal. The scoped project
must begin `lakehouse-ops-`; run/container identities must match the executor's UUID forms.
Only the exact requested launch is inspected; unrelated stages are not enumerated.

Output retains project, run, name and, when verified, immutable container ID.
`status: ready, action: removed` means the verified ID is now absent, not that a batch
or transaction recovered. Forced Docker removal may acknowledge an ID concurrently
removed by another actor. `already_absent` makes repeated operator calls safe. Refusals,
Docker timeouts/failures and unconfirmed absence exit 2. Requests are bounded: five
seconds for inspection, thirty for removal, five for absence confirmation.

The journal and processing checkpoint are not rewritten; the lock file is not deleted.
Capture stdout externally if durable operator-action evidence is required. There is no
automatic restart, bulk project cleanup, volume removal or checkpoint advancement.
Successful cleanup does not release the operator from inspecting committed Iceberg state
before a verified retry. Never reset a warehouse/checkpoint merely to get a green run.

## Fence and recovery limits

The OS lock protects normal cooperating runners in this host/checkout only. Another
checkout, host, direct Compose stage or external writer can bypass it. It does not prove
that a container is orphaned, establish distributed fencing, or roll back an active
Iceberg transaction. The explicit acknowledgement permits stopping a selected stage
even during a write; this is an operator decision, not an automatic safety inference.

The immutable-ID target prevents a concurrently replaced **name** from being removed.
Labels for the inspected immutable object are checked before mutation. Daemon uncertainty
is never treated as absence. The normal read-only inspector remains non-mutating.

## Local Docker acceptance

```powershell
$env:LAKEOPS_RUN_CONTAINER_CLEANUP_SMOKE = '1'
uv run --frozen pytest tests/integration/test_commerce_scoped_container_cleanup.py --no-cov
```

The bounded smoke starts a network-disabled 128 MiB Python sleeper using the existing
Spark image. Its owner records durable launch intent while holding the OS lock, starts
the container, then exits with `os._exit(9)` without unlocking through Python cleanup.
The parent verifies the stage still runs, proves a held lock blocks cleanup, then removes
only the recorded immutable ID and verifies an absent replay. A second experiment
replaces the container under the same name between inspection/removal: the replacement
must stay running and a later call must refuse its foreign run label. Journal bytes and
lock-file existence stay unchanged. Fixture cleanup removes only the temporary test IDs.

This proves local operator-cleanup plumbing after a real process exit and a name-reuse
race, not Spark commit interruption, row/file recovery or distributed exactly-once.
No public CLI contract, version, release or mandatory CI check is changed.

Fresh local evidence on 2026-10-12: both real process-exit/name-reuse experiments
passed; only their temporary container IDs were removed, with no volumes or existing
stack touched and no running test container left. JUnit:
`artifacts/container-cleanup-docker-20261012-v2.xml`, SHA256
`499acc9eccf05c53513650cb8cd2e226458a1f55ee40d634ca33b06e8e92ee81`.
The initial test expected Docker to reject an already absent ID, but this daemon's
forced removal acknowledged it. The corrected test binds its expected report to that
actual command return code and still requires the foreign replacement to survive.

Fresh one-command local gates on main including #207: 1039 passed, 20 opt-in skips,
91.70% coverage, all lint/policy/contract/image-lock checks and three additional real
Docker timeout/ownership/Spark-memory smokes. Lockfile and full Compose config checks pass.
