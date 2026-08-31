# Platform SLOs

The local observability profile evaluates three explicit service-level objectives from
live Prometheus samples. These objectives make the demo stack measurable; they are not
universal production targets.

| Objective | Indicator | Target | Evaluation |
| --- | --- | --- | --- |
| Query success | Trino failed queries divided by started queries | at least 99% successful | rolling 5 minutes with observed traffic |
| Ingestion freshness | age of the latest silver `ingested_at` value | at most 15 minutes | every 5 seconds |
| Maintenance backlog | Iceberg data files smaller than 128 MiB | at most 10 files | every 5 seconds |

The query objective uses Trino's cumulative started and failed query counters. A window
without query traffic is marked unobserved instead of silently passing. The ingestion
and maintenance objectives also require a successful operational collector scrape.
Prometheus records each indicator and the combined `lakehouse:slo:objectives_met`
result in `config/observability/slo.yml`.

## Acceptance check

Start the core and observability profiles after producing the silver table, then run:

```bash
docker compose --profile observability run --rm platform-slo-check
```

The checker waits for the recording rules to see live Trino traffic and exits zero only
when all three objectives are currently met. Inspect individual results with these
Prometheus expressions:

```promql
lakehouse:slo:query_success_ratio5m
lakehouse:slo:ingestion_freshness_compliant
lakehouse:slo:maintenance_backlog_compliant
lakehouse:slo:objectives_met
```

Sustained breaches produce warning alerts after five minutes for query success and
freshness, and after fifteen minutes for maintenance backlog. Tune the thresholds only
with representative workload evidence. A larger production table should normally use
a backlog ratio or maintenance-age objective rather than copying the demo's absolute
file-count threshold.

Counter resets after a Trino coordinator restart can make the rolling query indicator
temporarily unobserved. Wait for two Prometheus samples and new query traffic before
interpreting the result. The freshness objective describes pipeline recency, not source
event time or end-to-end correctness.
