from __future__ import annotations

import json
import sys
from pathlib import Path

EXPECTED_QUERIES = {
    "full_table_scan",
    "location_aggregate",
    "partition_time_filter",
}
EXPECTED_METRICS = {
    "state",
    "elapsed_time_ms",
    "wall_time_ms",
    "cpu_time_ms",
    "processed_rows",
    "processed_bytes",
    "physical_input_bytes",
    "peak_memory_bytes",
    "spilled_bytes",
}


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_trino_baseline.py REPORT")
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    queries = report.get("queries", [])
    query_ids = {query.get("query_id") for query in queries}
    trino_ids = {query.get("trino_query_id") for query in queries}
    checks = {
        "schema_version": report.get("schema_version") == "1.0",
        "status": report.get("status") == "ready",
        "engine": report.get("engine") == "trino",
        "mode": report.get("mode") == "explain_analyze",
        "corpus": report.get("corpus")
        == {
            "schema_version": "1.0",
            "name": "silver_weather_baseline",
            "query_count": 3,
        },
        "query_ids": query_ids == EXPECTED_QUERIES,
        "trino_query_ids": len(trino_ids) == 3
        and all(isinstance(value, str) and value for value in trino_ids),
        "digests": all(
            len(query.get("sql_sha256", "")) == 64
            and len(query.get("plan_sha256", "")) == 64
            and query.get("plan_line_count", 0) > 0
            for query in queries
        ),
        "metrics": all(_valid_metrics(query.get("metrics")) for query in queries),
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise SystemExit("Trino baseline evidence failed: " + ", ".join(failed))
    print(
        json.dumps(
            {
                "status": "ready",
                "corpus": "silver_weather_baseline",
                "queries": len(queries),
                "processed_bytes": sum(
                    query["metrics"]["processed_bytes"] for query in queries
                ),
            },
            sort_keys=True,
        )
    )


def _valid_metrics(value: object) -> bool:
    if not isinstance(value, dict) or set(value) != EXPECTED_METRICS:
        return False
    if value.get("state") != "FINISHED":
        return False
    return all(
        isinstance(metric, int) and not isinstance(metric, bool) and metric >= 0
        for key, metric in value.items()
        if key != "state"
    )


if __name__ == "__main__":
    main()
