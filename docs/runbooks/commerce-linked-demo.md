# One-command fresh-stack commerce demonstration

From the repository root, with Docker running and the reference images available:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_linked_demo `
  --output artifacts/commerce-linked-demo
```

Use a new empty output directory. The opt-in private module creates a unique
`lakehouse-ops-linked-demo-<uuid>` Compose project and probes four distinct local
ports. It starts MinIO, PostgreSQL-backed Hive Metastore and the two-node Trino
reference profile, then runs the existing linked-snapshot acceptance through
all production Spark stages and independent Trino revenue/history oracles.
After acceptance or a startup/child failure it stops only those five named
services in its own project. It never removes volumes, warehouses, checkpoints,
images or logs. Partially started services are also eligible for scoped shutdown.

The existing acceptance covers a new order, its changed amount, and a late order;
current revenue must be 1000, 1500 and 2200 cents, not their sum of 4700. Customer
periods must match the independent literal oracle before each checkpoint.
Conditional landing replay creates no new objects. Final current-mart replay must
add no snapshot, and the final checkpoint-driven runner must be idle without compute.
See [linked acceptance](commerce-linked-acceptance.md) for exact semantics.

The command retains `demo.json`, Docker/config/start/acceptance/stop logs and the
complete `acceptance/` directory with checkpoints, SQLite journal, source snapshots,
stage logs and `linked.json`. `demo.json` contains the unique project, selected
ports, command return codes and elapsed wall time. A nonzero child exit, missing
or failed acceptance report, or failed shutdown makes the demonstration fail
and exit 2. A successful child exit without retained acceptance is not success.
Native Docker/Spark stderr is included in `linked.log`, alongside child stdout.

## Prerequisites and isolation

The demo does not automatically build or pull images. Use the repository's normal
reference-stack preparation before running it. An explicit Spark-image preflight
must pass before startup, so a missing compute image cannot trigger a stage pull.
Infrastructure startup uses
`--no-build --pull never`; compute stages reuse the same locally available images.
No marketplace services or existing project are started, stopped or modified.
Missing images or unavailable Docker produce failed evidence, not a CI fallback.

Compose commands carry the generated project explicitly. The child environment
selects this repository's `compose.yaml`, so an inherited `COMPOSE_FILE` cannot
redirect stage execution to another checkout. Development ingestion identity and
credentials are selected consistently for MinIO and the S3 client; inherited AWS
session tokens and extra Compose profiles are removed from child environments.
Parent environment variables stay unchanged. These are development identities,
not a production credential-management design. Other ordinary reference-stack
Compose settings remain supported.

Ports are free at probe time; they are released before Docker binds them. Another
process can win that race, so availability is not guaranteed. The demo retains
the startup failure and stops its own partially created stack instead of choosing
another catalog and hiding the failure. It does not reserve ports permanently.

Command waits are bounded: Docker/Spark-image/config 30 seconds each, infrastructure startup
240 seconds (Compose readiness 180), linked acceptance 1800 and shutdown 120.
Compute stages receive the existing 300-second stage timeout. The repository's
normal runner lock remains authoritative; an active runner can reject the demo.
Outer process waiting limits are not guaranteed process-tree termination. If a
child is forcibly terminated or shutdown fails, inspect only the recorded project
and its owned one-off containers. Do not prune Docker or remove retained volumes.

## Scope and release discipline

This is an executable demonstration of the existing bounded single-writer,
three-full-snapshot scenario with isolated stack lifecycle. It does not add
general CDC/deletes, retroactive customer-history corrections, kill-inside-commit
recovery, capacity measurements or a release-candidate evidence bundle. The
separate recovery driver remains separate. No version or public CLI change.
Mandatory exact-head CI, resolved reviews and conflict-free state are still
required before squash merge; release policy remains unchanged.

Rollback: stop using the opt-in demo module and use the manual linked acceptance
runbook. Existing default pipeline and scheduled runner behavior are unchanged.

## Retained local execution, 2026-10-12

Initial full execution completed on one fresh project in 387.48 seconds. Final
source, including Spark-image preflight, completed on a second fresh project in
383.17 seconds: infrastructure startup 41.44, linked acceptance 327.83 and scoped
shutdown 12.74 seconds. These are warm-image local observations, not timing or
capacity guarantees. Both runs observed 1000/1500/2200 cents and exact customer
periods, snapshot-stable current-mart replay, and compute-free idle behavior.

The final project was `lakehouse-ops-linked-demo-dd92e8570d2249d8ad09e1c43274179e`.
Read-only journal inspection observed three ready runs and three completed
checkpoints. After shutdown, no containers in that project were running; all six
project volumes remained present. A separate unavailable-Docker run retained a
failed first-step report and exited 2 without startup or shutdown commands.

Fresh lint, access-policy, stable contract and image-lock gates passed. The final
default suite passed 965 tests with 13 expected opt-in skips and 91.16% coverage
in 34.10 seconds. Sixteen new unit cases cover project/environment isolation,
preflight/start/acceptance/shutdown failures, child timeout/launch failure, missing
or failed retained acceptance, non-overwrite and entrypoint behavior.

Final evidence directory: `artifacts/commerce-linked-demo-20261012-v2/`.
`demo.json` SHA256:
`b6834ff88401e6907b6a94c12b12cc6d08362925cd9f110e29ad917a90a702fa`.
JUnit: `artifacts/commerce-linked-demo-quality-20261012-v2.xml`, SHA256
`8c210e5240dd74aff993d10706f7a4ee4f7688f2e4e39cd28e257319415aadcd`.
The failed-Docker evidence is retained separately under
`artifacts/commerce-linked-demo-20261012-unavailable-v2/`.
Logs, catalogs, source snapshots and journals are retained locally, not shipped
release assets. No failed warehouse or checkpoint was reset.
