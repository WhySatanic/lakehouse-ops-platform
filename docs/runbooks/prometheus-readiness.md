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
docker compose --profile observability up -d blackbox-exporter prometheus
docker compose --profile observability run --rm prometheus-check
```

Prometheus is available at `http://localhost:9090` by default. Query
`probe_success` to inspect each target. A value of `1` means the probe succeeded;
`0` means the boundary is unavailable. The checker retries for two minutes to allow
initial scrapes, then exits non-zero and lists targets without a healthy sample.

This profile intentionally does not claim application-level correctness, dashboards,
alert delivery, or SLO coverage. Use the existing Spark and Trino integration checks
to prove data correctness. Stop the optional profile with:

```bash
docker compose --profile observability down
```

Changing `PROMETHEUS_PORT` or `BLACKBOX_EXPORTER_PORT` only changes host publishing;
the internal scrape addresses remain stable.
