# Spark container memory observations

Implemented opt-in diagnostics, not a full-pipeline volume benchmark. The production
Spark session helper registers one stderr JSON observation on normal Python exit.
Default job output and the public CLI contract remain unchanged. With diagnostics
enabled, the Spark launcher can forward Python stderr to its stdout, so retain both
container streams. The commerce runner routes stage stdout into its own stderr log.

Enable all commerce Spark stages, including the optional current daily mart:

```powershell
$env:COMPOSE_FILE = 'compose.yaml;compose.spark-memory.yaml'
uv run lakeops run-commerce-batch --help
# Run the usual configured batch command; retain its stderr log as evidence.
Remove-Item Env:COMPOSE_FILE
```

On Linux use `COMPOSE_FILE=compose.yaml:compose.spark-memory.yaml`. Startup, landing,
credentials, Trino verification and processing checkpoint follow the existing
[commerce runbook](commerce-training-fixture.md). The overlay starts no services itself.
Use a dedicated Compose project and private cgroup namespace for reproducible runs.
Do not mix benchmark evidence with unrelated containers or change their limits.

Each `spark_container_memory` line records app name, selected batch ID, elapsed time
since session initialization began, peak bytes and current bytes at Python exit.
The Linux [cgroup-v2 memory controller](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)
defines `memory.peak` for the cgroup and descendants. This includes the container's
JVM, Python and accounted cache/kernel memory, not only Python heap or JVM heap.
The peak covers the container lifetime, including before hook registration; elapsed
time does not. Readings are sequential, not an atomic snapshot.

Support is deliberately limited to private cgroup-v2 containers exposing `0::/` and
readable `memory.peak`/`memory.current`. Unsupported layouts, controllers and malformed
readings report `unavailable` with null values, never fabricated zero usage. Do not
use this helper directly on a host root cgroup. No files are reset or written.
SIGKILL, OOM kill and abrupt process exit can prevent emission; a missing report must
remain missing evidence, not success. A report itself does not prove job success.
Remote executors, HMS, MinIO and Trino memory are outside this scope.

Local real-Spark acceptance (prebuilt open-source image, no network):

```powershell
$env:LAKEOPS_RUN_SPARK_MEMORY_SMOKE = '1'
uv run pytest tests/integration/test_spark_memory.py -q -s --no-cov
Remove-Item Env:LAKEOPS_RUN_SPARK_MEMORY_SMOKE
```

It writes and reads 100,000 Parquet rows through the production session helper and
requires one positive observed container-memory report. It is not an HMS/S3/Trino
end-to-end test. Next measurement work must retain complete commerce stage timings,
these per-container peaks, Iceberg file counts, Trino queries and recovery outcomes
at multiple volumes before making pipeline capacity claims.

Upgrade/rollback: no data migration. Omit the overlay (and leave
`LAKEOPS_SPARK_MEMORY_REPORT` unset) to restore the default silent behavior.
