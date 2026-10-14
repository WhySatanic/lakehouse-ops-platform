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
After acceptance or a startup/child failure it runs scoped Compose `down` for its
unique project, removing stopped service and completed initializer containers.
It never passes `--volumes` or `--rmi`: named warehouse volumes, checkpoints,
images and retained evidence stay intact. Partially started services are also
eligible for scoped shutdown. The existing `stop` step/log name is preserved.
After successful teardown it inspects its exact `<project>_default` network and
releases it only when its name and Compose project label match and it has no
attached containers. This prevents repeated demonstrations from consuming all
Docker subnet pools. The network normally is already absent after `down`.
Compose recreates containers and the network using the retained named volumes
when that exact project is started again. No global prune is used. Container-local
Docker logs are removed with their containers; acceptance/stage logs are retained.

The existing acceptance covers a new order, its changed amount, and a late order;
current revenue must be 1000, 1500 and 2200 cents, not their sum of 4700. Customer
periods must match the independent literal oracle before each checkpoint.
Conditional landing replay creates no new objects. Final current-mart replay must
add no snapshot, and the final checkpoint-driven runner must be idle without compute.
See [linked acceptance](commerce-linked-acceptance.md) for exact semantics.

## Late arrival during an unfinished publication

The same demo can reproduce the in-flight queue recovery path on a fresh stack:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_linked_demo `
  --inflight-late-arrival --output artifacts/commerce-inflight-demo
```

It initially lands only A (February 1) and C (February 3). After A completes,
C publishes one real Iceberg SCD2 commit. Independent Trino SQL verifies the
Alice/Alice Updated periods ending/starting on February 3 and exactly one current
version. The callback then lands full snapshot B (February 2) in MinIO and raises
a dedicated caught exception before C's batch gold/current gold/checkpoint.
C's failed attempt must leave checkpoint bytes unchanged.

A fresh planner reads the persisted active intent and retries C through all nine
normal stages. C's history replay must add no snapshot. Only its verified completion
records C's checkpoint; B then runs all nine stages with retained-history rebuild.
That rebuild must add exactly one history commit, moving Alice Updated's boundary
back to February 2. B's batch-scoped revenue is 1500 cents; the newer current result
must remain C's two orders / 2200 cents, not regress to B. A new ordinary February 4
snapshot then completes in forward mode, publishes its own current identity without
adding customer history, and leaves the runner compute-free idle.

`acceptance/linked.json` retains `inflight_recovery`: five distinct journal attempts
(four complete and one injected failure), published C's literal history, snapshot
counts, per-completion batch/current/history SQL results, before/after failure
checkpoint SHA256, final four completed identities and idle. The existing demo's
scoped teardown and retained-data rules apply unchanged. Normal linked and manual
history-correction commands retain their original scenarios. The in-flight option
cannot be combined with manual maintenance, profiling or owner-exit drills.

This opt-in two-order correctness drill needs the active-intent queue fix. It tests
a caught failure after actual SCD2 publication, not process death, concurrent writers,
arbitrary partial-table recovery, repeated performance measurements or RC evidence.
Serving CI executes this same command alongside the unchanged normal history-correction
demo, then rechecks the retained report/checkpoint/journal read-only. Both paths' evidence
is uploaded even when a step fails. This reuses the existing 60-minute serving job;
the established normal path took about 25 minutes in the fix PRs. CI evidence alone
does not replace the release policy's clean-checkout RC bundle and attestation.

The command retains `demo.json`, Docker/config/start/acceptance/stop logs and the
complete `acceptance/` directory with checkpoints, SQLite journal, source snapshots,
stage logs and `linked.json`. `demo.json` contains the unique project, selected
ports, command return codes and elapsed wall time. A nonzero child exit, missing
or failed acceptance report, or failed shutdown makes the demonstration fail
and exit 2. A successful child exit without retained acceptance is not success.
Native Docker/Spark stderr is included in `linked.log`, alongside child stdout.
`network_cleanup` records the released network, or `absent` when startup never
created it. Inspection or removal failure makes the demo fail with `cleanup_error`.
An active or differently owned network is retained, never forcibly disconnected.

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
Network inspection and removal each have a separate 30-second bound after stop.
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

The linked, late-history, owner-exit and profile demos share this teardown logic.
Their commands, source processing and retained warehouse data are unchanged.

Local container-removal verification, 2026-10-14: all seven local quality gates
passed (1250 tests, 29 opt-in skips, 92.32% package coverage). Four real Docker
smokes passed in 54.41 seconds. The new lifecycle cases exercised successful
acceptance, failed startup after service creation, and child failure using small
sleepers instead of a full Spark pipeline. In every case service and completed
initializer containers and the network were absent after teardown, a marker in
the named volume remained readable, and the same project could restart and be
removed again without deleting that volume. This is lifecycle evidence, not a
new full-pipeline benchmark or release-candidate proof.

Exercise the actual network release without running another Spark pipeline:

```powershell
$env:LAKEOPS_RUN_DEMO_NETWORK_SMOKE = "1"
uv run --frozen pytest tests/integration/test_commerce_demo_network_smoke.py -q -s -o addopts=
```

The smoke creates a uniquely owned network, one temporary sleeper and an empty
volume. It refuses release while the endpoint is active, removes its own sleeper,
releases only the empty network and verifies the volume still exists. The empty
volume remains under the printed test-project name; no pre-existing data is changed.

Local teardown verification, 2026-10-13: the real network smoke passed in 3.84
seconds. Retained volume:
`lakehouse-ops-linked-demo-5d50278b153e47feaac1f13db98a0b1a_data`.
The shared quality gates passed with 1201 tests, 25 opt-in skips and 92.27% coverage;
94 focused demo/network tests also passed. Logs and JUnit:
`artifacts/demo-teardown/{quality-v2.log,network-smoke.log,network-smoke.xml}`.
This proves the teardown behavior, not another full Spark run or release readiness.

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
