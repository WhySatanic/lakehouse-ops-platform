# One-command forced-owner-exit demonstration

With local Docker running and the reference images prepared, run from the checkout root:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_owner_exit_demo `
  --output artifacts/commerce-owner-exit-demo
```

Choose a new empty output directory. This opt-in private module reuses the linked demo's
stack lifecycle, but selects the existing forced-owner-exit driver and its own unique
`lakehouse-ops-owner-exit-demo-<uuid>` project. It probes four distinct ports, starts
MinIO, PostgreSQL-backed Hive Metastore and two-node Trino, then runs all eight commerce
stages in a spawned owner process. After successful gold publication, the driver kills
only that owner, reacquires its normal OS lock and retries the same batch with a new
attempt ID. No checkpoint may exist before retry. All 14 audited table row/current-file
hashes must remain unchanged, and the final invocation must be idle without compute.

This is recovery **after completed gold**, not recovery during a Spark/Iceberg commit.
See [forced owner exit](commerce-owner-exit.md) for the exact boundary and invariants.
The new launcher does not change that acceptance, pipeline or journal semantics.

## Isolation, evidence and failures

Docker availability, the local Spark image and Compose configuration are checked first.
Infrastructure startup uses `--no-build --pull never`. The child uses this checkout's
Compose file, consistent public development ingestion credentials and no inherited AWS
session token or extra Compose profile. The parent environment is not modified. Port
probes release their sockets before startup, so races can still cause a retained failure.

After successful acceptance or startup/child failure, shutdown targets only the five
persistent services in the generated project. No global prune, volume deletion, warehouse
reset or checkpoint rewrite occurs. Existing projects, including marketplace, are not
started or stopped. Project volumes remain available for diagnosis.

The output retains `demo.json`, preflight/config/start/owner-exit/stop logs and the full
`acceptance/` directory, including `owner-exit.json`, SQLite journal, checkpoint, fixture
and owner/retry logs. A zero child exit without a ready acceptance report is failure.
Missing/malformed reports, nonzero exits, launch errors, timeouts and failed shutdown
produce a failed demo and exit code 2. An unrelated root or nonempty output is rejected.

Waits are bounded: three preflights at 30 seconds each, infrastructure startup 240 seconds
(readiness 180), owner-exit acceptance 6600 seconds, shutdown 120. Each compute stage has
the existing 300-second timeout. The larger acceptance limit leaves time for the driver's
1800-second boundary wait and a complete eight-stage retry; it is not a performance SLA.
The normal runner lock still prevents concurrent local writers.

Outer subprocess timeout is not process-tree termination. If termination happens before
the completed-gold marker, an owned one-off container can remain active even after the
five services stop. Inspect only the recorded project and verify that no writer remains
before another attempt. This launcher does not claim orphan cleanup or automatic restart.

## Compatibility

The default linked demo remains unchanged; the owner scenario is explicitly selected by
a separate private module, not a new stable control-plane command. Existing manual
acceptance remains available. Stop using this module to roll back. No version bump,
release, release-candidate bundle or CI check removal is included.

## Retained local execution, 2026-10-12

Fresh project `lakehouse-ops-owner-exit-demo-ffd7b44e41694bf38f1df15b1d6a2624`
completed on Windows Docker/WSL2: startup 40.98 seconds, owner-exit acceptance 202.41,
shutdown 11.73. These are warm-image local observations, not capacity or timing guarantees.
The owner exited with `-15`; checkpoint before retry was absent. The retained journal
contains 17 incomplete owner events and 23 successful retry events with distinct attempt
IDs. Its read-only inspection smoke passed. All 14 table row/current-file hashes stayed
unchanged, retry verified 100 orders, 31 days and 278410 captured cents, and the last
runner call was idle without compute.

After shutdown no project containers were running, and all six project volumes remained.
A separate unavailable-Docker invocation retained one failed preflight, exited 2 and
issued no startup/shutdown commands. Full local quality: 1006 passed, 16 expected skips,
91.46% coverage, plus three real Docker smoke tests. Both launch scenarios and entrypoints
have 33 focused lifecycle cases; no new full-stack CI job is introduced.

Evidence: `artifacts/commerce-owner-demo-20261012/demo.json`, SHA256
`4f99d2f207d1fc6511699bc686162095ba7ef5528f6ae45f3989e220403544ea`;
`acceptance/owner-exit.json`, SHA256
`2b66969e566c3866ea20425b765b43f8a14995190bd15bf891e4f4521a72d317`.
Unavailable-Docker evidence: `artifacts/commerce-owner-demo-unavailable-20261012/`.
These retained local diagnostics are not release assets. This launcher is independent
of the separate container-ownership propagation change and does not require it to run.
