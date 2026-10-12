# One-command local quality gate

From the repository root after `uv sync --frozen --dev`, run:

```powershell
uv run --frozen python -m lakehouse_ops.local_quality
```

This checks the lock, Ruff, stable CLI/JSON contract, generated Trino access policy,
external image digest lock, all core Compose profiles and the full pytest suite with
the existing coverage threshold. No gate is disabled and no container is started,
stopped or built. Docker Compose must be installed; configuration rendering does not
require the Docker engine. The checks use frozen dependencies rather than changing
the lockfile. Tests create their normal temporary files and coverage outputs.

Logs go to stderr; stdout is one JSON report with each attempted gate's status,
exit code, failure reason and elapsed time. Exit 0 means all seven gates passed;
exit 1 means a gate failed and later gates were not run; invalid arguments/root exit 2.
By default each command has a 300-second limit, overridable with
`--timeout-seconds 600` (1..3600). A timeout is a failure, not evidence that descendant
processes stopped; inspect them before rerunning. No command starts infrastructure.

The scope is explicitly `local_quality_not_integration`. Opt-in smoke checks still
need their own environment and a running isolated stack. This does not replace
mandatory remote CI, prove Spark/HMS/Trino integration, measure full-pipeline capacity
or permit merging a PR whose checks are pending. Use it before publishing an increment
to catch inexpensive failures locally, then continue independent work while CI runs.
