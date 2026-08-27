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
  alert-webhook alertmanager blackbox-exporter prometheus grafana
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

Prometheus evaluates `LakehouseCoreTargetDown` when any configured readiness target
reports `probe_success == 0` for 30 seconds. Alertmanager groups the alert by target
and delivers it to the local webhook receiver. The CI acceptance drill stops the
Trino coordinator, verifies the exact firing alert, restores Trino, verifies all
targets are healthy again, and then requires the matching resolved notification.
The webhook receiver is an executable local test boundary, not a production pager.

This profile intentionally does not claim application-level correctness, workload
dashboards, production paging, or SLO coverage. Use the existing Spark and Trino
integration checks to prove data correctness. Stop the optional profile with:

```bash
docker compose --profile observability down
```

Changing `PROMETHEUS_PORT`, `BLACKBOX_EXPORTER_PORT`, or `GRAFANA_PORT` only changes
host publishing; the internal datasource and scrape addresses remain stable.
