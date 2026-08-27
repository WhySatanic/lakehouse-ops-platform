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
docker compose --profile observability up -d blackbox-exporter prometheus grafana
docker compose --profile observability run --rm prometheus-check
docker compose --profile observability run --rm grafana-check
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

This profile intentionally does not claim application-level correctness, workload
dashboards, alert delivery, or SLO coverage. Use the existing Spark and Trino integration checks
to prove data correctness. Stop the optional profile with:

```bash
docker compose --profile observability down
```

Changing `PROMETHEUS_PORT`, `BLACKBOX_EXPORTER_PORT`, or `GRAFANA_PORT` only changes
host publishing; the internal datasource and scrape addresses remain stable.
