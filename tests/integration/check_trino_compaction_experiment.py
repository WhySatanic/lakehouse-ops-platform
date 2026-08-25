from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

METRICS = {
    "elapsed_time_ms",
    "wall_time_ms",
    "cpu_time_ms",
    "processed_rows",
    "processed_bytes",
    "physical_input_bytes",
    "peak_memory_bytes",
    "spilled_bytes",
}


def validate(report: dict[str, Any]) -> list[str]:
    layout = report.get("file_layout", {})
    before = layout.get("before", {})
    after = layout.get("after", {})
    workload = report.get("workload", {})
    medians = report.get("medians", {})
    comparison = report.get("comparison", {})
    checks = {
        "schema_version": report.get("schema_version") == "1.0",
        "status": report.get("status") == "ready",
        "experiment": report.get("experiment") == "iceberg_data_file_compaction",
        "table": report.get("table") == "lakehouse.ops.maintenance_fixture",
        "snapshots": report.get("snapshots", {}).get("before")
        != report.get("snapshots", {}).get("after"),
        "before_layout": before.get("data_file_count") == 4
        and before.get("record_count") == 100_000,
        "after_layout": isinstance(after.get("data_file_count"), int)
        and 0 < after["data_file_count"] < 4
        and after.get("record_count") == 100_000,
        "file_reduction": layout.get("file_count_reduction")
        == before.get("data_file_count", 0) - after.get("data_file_count", 0)
        and layout.get("file_count_reduction_percent", 0) > 0,
        "workload": workload.get("mode") == "explain_analyze"
        and workload.get("repetitions") == 3
        and len(workload.get("sql_sha256", "")) == 64,
        "medians": set(medians.get("before", {})) == METRICS
        and set(medians.get("after", {})) == METRICS
        and _valid_metrics(medians["before"])
        and _valid_metrics(medians["after"]),
        "comparison": set(comparison) == METRICS
        and all(
            _valid_comparison(
                value,
                medians.get("before", {}).get(metric),
                medians.get("after", {}).get(metric),
            )
            for metric, value in comparison.items()
        ),
        "latency_observation": report.get("latency_observation")
        in {"improved", "unchanged", "regressed"},
    }
    return [name for name, passed in checks.items() if not passed]


def _valid_metrics(value: dict[str, Any]) -> bool:
    return all(
        isinstance(metric, int) and not isinstance(metric, bool) and metric >= 0
        for metric in value.values()
    )


def _valid_comparison(value: object, before: object, after: object) -> bool:
    if not isinstance(value, dict):
        return False
    if not isinstance(before, int) or not isinstance(after, int):
        return False
    return (
        value.get("before") == before
        and value.get("after") == after
        and value.get("delta") == after - before
    )


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_trino_compaction_experiment.py REPORT")
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    failed = validate(report)
    if failed:
        raise SystemExit("Trino compaction experiment failed: " + ", ".join(failed))
    print(
        json.dumps(
            {
                "status": "ready",
                "files_before": report["file_layout"]["before"]["data_file_count"],
                "files_after": report["file_layout"]["after"]["data_file_count"],
                "latency_observation": report["latency_observation"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
