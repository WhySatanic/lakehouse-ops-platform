# One-command local commerce check

From the repository root, with Docker running and the reference Spark image built:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_local_check `
  --output artifacts/commerce-local-check-demo
```

Use a new empty output directory for each run. The opt-in private module executes
Docker/image preflight, repository-wide lint, generated access-policy check, stable
control-plane contract check, image-lock verification, the full default test suite
with coverage, and two actual Docker smoke tests. No public CLI contract changes.
It uses the existing open-source `lakehouse-ops/spark-iceberg:3.5.9-1.11.0` image.
If the image is missing, build only the reference compute image:

```powershell
docker compose --profile compute build spark-bronze
```

The smoke tests exercise two real stage timeouts with owned-container cleanup and
a real local Spark JVM/Python Parquet write/read with cgroup-v2 memory observation.
They use network-disabled, disposable containers, not existing MinIO/Trino tables.
No existing stack is started/stopped, no volumes are removed, and no images are
pulled or built by the checker. A unique scoped Compose project name is supplied.
Inherited `LAKEOPS_RUN_*` opt-ins are removed from child environments; only the two
explicit Docker smoke flags are enabled for that step. Inherited `PYTEST_ADDOPTS`
is removed so collection-only or test-filtering options cannot replace the suite.
The parent's environment
is unchanged. The default suite's documented opt-in skips remain expected.

`check.json` records each attempted command, return code and elapsed wall time.
Each step has a separate combined-output log; `quality.xml` and `docker-smoke.xml`
retain test evidence. A failed/failed-to-launch/timed-out command stops subsequent
steps, retains evidence, and exits 2. Missing, malformed or skipped Docker smoke
evidence is not reported as success, even if pytest exits zero.

Command limits are 30 seconds for preflight, 120 for static checks, 600 for the
full suite, and 300 for the combined Docker smoke step. These bound child command
waiting, not an entire process tree or guaranteed cleanup after forced termination.
Inspect the retained log and owned containers if a Docker test fails.

This is a short local feedback path, not the full eight-stage commerce acceptance,
three-load correction scenario, crash recovery or release evidence bundle. It does
not replace or disable any mandatory CI job. Squash merge still requires all
mandatory checks on the exact latest PR SHA, conflict-free state and resolved reviews.

## Retained local execution, 2026-10-12

Final source execution completed all eight commands in 48.79 seconds, including
930 passing default-suite tests (10 expected opt-in skips), 90.74% coverage, and
two executed Docker smoke tests in 14.15 seconds. The timeout test verified two
distinct owned containers were removed; the Spark test performed real Parquet
write/read and observed 544722944 peak container bytes. This is one warm local
observation, not a runtime guarantee or a capacity benchmark.

Evidence directory: `artifacts/commerce-local-check-20261012-v2/`.
`check.json` SHA256:
`75f32e6fb3d4263f2553342667e58ef3250b5b944b3f24d1fd663767934a0c8c`.
Logs and both JUnit reports are retained locally, not shipped release assets.
