from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

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
TOTAL_ROWS = 65_536
TARGET_ROWS = 128


def non_negative_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def valid_hash(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64


def valid_table(value: object, *, name: str, sorted_by: bool) -> bool:
    if not isinstance(value, dict):
        return False
    return (
        value.get("table") == f"lakehouse.ops.{name}"
        and isinstance(value.get("snapshot_id"), str)
        and value["snapshot_id"].isdigit()
        and value.get("record_count") == TOTAL_ROWS
        and value.get("partition_count") == 1
        and isinstance(value.get("data_file_count"), int)
        and value["data_file_count"] >= 8
        and isinstance(value.get("total_size_bytes"), int)
        and value["total_size_bytes"] > 0
        and valid_hash(value.get("create_sql_sha256"))
        and value.get("sorted_by_event_id") is sorted_by
    )


def valid_runs(value: object) -> bool:
    if not isinstance(value, list) or len(value) != 3:
        return False
    if {run.get("repetition") for run in value if isinstance(run, dict)} != {1, 2, 3}:
        return False
    for run in value:
        if not isinstance(run, dict):
            return False
        metrics = run.get("metrics")
        if (
            not isinstance(run.get("trino_query_id"), str)
            or not valid_hash(run.get("plan_sha256"))
            or not isinstance(metrics, dict)
            or set(metrics) != METRICS | {"state"}
            or metrics.get("state") != "FINISHED"
            or not all(non_negative_integer(metrics.get(metric)) for metric in METRICS)
        ):
            return False
    return True


def valid_medians(report: dict[str, object], variant: str) -> bool:
    runs = report.get("runs", {}).get(variant, [])
    medians = report.get("medians", {}).get(variant, {})
    if not isinstance(medians, dict) or set(medians) != METRICS:
        return False
    return all(
        medians[metric]
        == int(statistics.median(run["metrics"][metric] for run in runs))
        for metric in METRICS
    )


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_trino_sort_order.py REPORT.json")
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    tables = report.get("tables", {})
    runs = report.get("runs", {})
    medians = report.get("medians", {})
    comparison = report.get("comparison", {})
    evidence = report.get("pruning_evidence", {})
    workload = report.get("workload", {})

    comparisons_match = set(comparison) == METRICS and all(
        comparison[metric].get("before") == medians["baseline"][metric]
        and comparison[metric].get("after") == medians["sorted"][metric]
        and comparison[metric].get("delta")
        == medians["sorted"][metric] - medians["baseline"][metric]
        for metric in METRICS
    )
    processed_before = medians.get("baseline", {}).get("processed_rows", 0)
    processed_after = medians.get("sorted", {}).get("processed_rows", 0)
    bytes_before = medians.get("baseline", {}).get("physical_input_bytes", 0)
    bytes_after = medians.get("sorted", {}).get("physical_input_bytes", 0)
    expected_processed_reduction = round(
        (processed_before - processed_after) * 100 / processed_before, 2
    )
    expected_bytes_reduction = round((bytes_before - bytes_after) * 100 / bytes_before, 2)

    checks = {
        "schema": report.get("schema_version") == "1.0",
        "status": report.get("status") == "ready",
        "experiment": report.get("experiment") == "iceberg_sort_order",
        "engine": report.get("engine") == "trino",
        "range": report.get("range")
        == {"start": 30_000, "end_exclusive": 30_128, "size": TARGET_ROWS},
        "predicate_hash": valid_hash(report.get("predicate_sha256")),
        "workload": workload.get("mode") == "explain_analyze"
        and workload.get("repetitions") == 3
        and valid_hash(workload.get("sql_template_sha256")),
        "baseline_table": valid_table(
            tables.get("baseline"), name="sort_baseline", sorted_by=False
        ),
        "sorted_table": valid_table(
            tables.get("sorted"), name="sort_ordered", sorted_by=True
        ),
        "filtered_result": report.get("filtered_result", {}).get("row_count")
        == TARGET_ROWS,
        "baseline_runs": valid_runs(runs.get("baseline")),
        "sorted_runs": valid_runs(runs.get("sorted")),
        "baseline_medians": valid_medians(report, "baseline"),
        "sorted_medians": valid_medians(report, "sorted"),
        "comparisons": comparisons_match,
        "processed_rows_reduced": 0 < processed_after < processed_before,
        "physical_input_reduced": 0 < bytes_after < bytes_before,
        "processed_evidence": evidence.get("processed_rows_reduced") is True
        and evidence.get("processed_rows_reduction_percent")
        == expected_processed_reduction,
        "bytes_evidence": evidence.get("physical_input_bytes_reduced") is True
        and evidence.get("physical_input_bytes_reduction_percent")
        == expected_bytes_reduction,
        "latency_observation": report.get("latency_observation")
        in {"improved", "unchanged", "regressed"},
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise SystemExit(f"sort order evidence failed: {', '.join(failed)}")
    print(
        json.dumps(
            {
                "status": "ready",
                "processed_rows_reduction_percent": expected_processed_reduction,
                "physical_input_bytes_reduction_percent": expected_bytes_reduction,
                "latency_observation": report["latency_observation"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
