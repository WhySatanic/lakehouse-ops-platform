# Prometheus core readiness

The optional `observability` profile collects black-box readiness signals for the
three core service boundaries:

- MinIO HTTP health endpoint;
- Hive Metastore TCP endpoint;
- Trino coordinator HTTP information endpoint.

Start the lakehouse services first, then start the collectors and run the bounded
acceptance check:

```bash
docker compose --profile catalog --profile compute up -d --wait minio metastore-db hive-metastore
docker compose --profile query up -d --wait trino-coordinator trino-worker trino-worker-2
docker compose --profile observability up -d \
  alert-webhook alertmanager blackbox-exporter lakehouse-metrics-exporter prometheus grafana
docker compose --profile observability run --rm prometheus-check
docker compose --profile observability run --rm grafana-check
docker compose --profile observability run --rm grafana-workload-check
docker compose --profile observability run --rm grafana-operational-check
```

Prometheus is available at `http://localhost:9090` by default. Query
`probe_success` to inspect each target. A value of `1` means the probe succeeded;
`0` means the boundary is unavailable. The checker retries for two minutes to allow
initial scrapes, then exits non-zero and lists targets without a healthy sample.

Grafana is available at `http://localhost:3000` by default. Sign in with the
development credentials from `.env.example` and open the `Lakehouse` folder. The
`Lakehouse Core Readiness` dashboard is provisioned automatically and displays both
the aggregate readiness state and the history for each target. The Grafana checker
requires the exact dashboard UID and verifies that the Prometheus datasource is healthy.

Prometheus also scrapes Trino's native OpenMetrics endpoint with the dedicated
`lakehouse-observer` identity. That identity can read system information but receives no
catalog or table grants. The `Lakehouse Trino Workload` dashboard displays running,
queued, and resource-waiting queries, five-minute started/completed/failed query rates,
and the derived failure percentage. The workload checker requires the exact provisioned
dashboard, a healthy datasource, an up Trino scrape target, and at least one observed
started query. Run a normal Trino query before invoking it outside the integration flow.

The lightweight `lakehouse-metrics-exporter` queries Trino on every Prometheus scrape
and exposes the current row count, latest ingestion timestamp, freshness age, Iceberg
data-file count, files below 128 MiB, and snapshot count for
`lakehouse.silver.weather_hourly`. The `Lakehouse Maintenance and Freshness` dashboard
shows those live values. Its checker requires the exact provisioned dashboard, a healthy
datasource and exporter target, a successful collection, and non-empty table metadata.
The exporter uses the read-only `lakehouse-operational-metrics` identity and the bounded
`global.operational` resource group, so slow metric queries cannot fill the ad-hoc queue.

Prometheus evaluates `LakehouseCoreTargetDown` when any configured readiness target
reports `probe_success == 0` for 30 seconds. Alertmanager groups the alert by target
and delivers it to the local webhook receiver. The CI acceptance drill stops the
Trino coordinator, verifies the exact firing alert, restores Trino, verifies all
targets are healthy again, and then requires the matching resolved notification.
The webhook receiver is an executable local test boundary, not a production pager.

This profile intentionally does not claim application-level correctness, production
paging, or SLO coverage. The freshness age describes the most recent `ingested_at`
value, and the small-file backlog is inventory rather than an automatic maintenance
decision. Query counters reset whenever the Trino coordinator restarts, and five-minute
rates need at least two Prometheus samples. Use the existing Spark and Trino integration
checks to prove data correctness.
Stop the optional profile with:

```bash
docker compose --profile observability down
```

Changing `PROMETHEUS_PORT`, `BLACKBOX_EXPORTER_PORT`, `LAKEHOUSE_METRICS_PORT`, or
`GRAFANA_PORT` only changes host publishing; the internal datasource and scrape
addresses remain stable.
