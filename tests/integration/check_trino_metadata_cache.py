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


def valid_hash(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64


def valid_run(run: object, cycle: int) -> bool:
    if not isinstance(run, dict):
        return False
    result = run.get("result", {})
    metrics = run.get("metrics", {})
    return (
        run.get("cycle") == cycle
        and isinstance(run.get("trino_query_id"), str)
        and result.get("data_file_count", 0) >= 32
        and result.get("record_count") == 65_536
        and result.get("total_size_bytes", 0) > 0
        and set(metrics) == METRICS | {"state"}
        and metrics.get("state") == "FINISHED"
        and all(
            isinstance(metrics.get(metric), int)
            and not isinstance(metrics.get(metric), bool)
            and metrics[metric] >= 0
            for metric in METRICS
        )
    )


def valid_phase(report: dict[str, object], variant: str, phase: str) -> bool:
    runs = report.get("runs", {}).get(variant, {}).get(phase, [])
    medians = report.get("medians", {}).get(variant, {}).get(phase, {})
    return (
        isinstance(runs, list)
        and len(runs) == 3
        and all(valid_run(run, cycle) for cycle, run in enumerate(runs, 1))
        and set(medians) == METRICS
        and all(
            medians[metric]
            == int(statistics.median(run["metrics"][metric] for run in runs))
            for metric in METRICS
        )
    )


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_trino_metadata_cache.py REPORT.json")
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    configuration = report.get("configuration", {})
    workload = report.get("workload", {})
    resets = report.get("resets", [])
    observation = report.get("cache_observation", {})
    comparisons = report.get("comparisons", {})

    checks = {
        "schema": report.get("schema_version") == "1.0",
        "status": report.get("status") == "ready",
        "experiment": report.get("experiment") == "iceberg_metadata_cache",
        "engine": report.get("engine") == "trino",
        "table": report.get("table") == "lakehouse.ops.pruning_partitioned",
        "snapshot": isinstance(report.get("snapshot_id"), str)
        and report["snapshot_id"].isdigit(),
        "configuration": valid_hash(configuration.get("enabled_sha256"))
        and valid_hash(configuration.get("disabled_sha256"))
        and configuration.get("only_difference")
        == "iceberg.metadata-cache.enabled"
        and configuration.get("cache_ttl") == "default:1h"
        and configuration.get("cache_max_size") == "default:2% coordinator heap"
        and configuration.get("cache_max_content_length") == "default:15MB",
        "workload": workload.get("cycles") == 3
        and workload.get("phases") == ["cold", "warm"]
        and valid_hash(workload.get("sql_sha256"))
        and workload.get("result", {}).get("data_file_count", 0) >= 32
        and workload.get("result", {}).get("record_count") == 65_536,
        "resets": isinstance(resets, list)
        and len(resets) == 3
        and all(
            isinstance(reset, dict)
            and reset.get("cycle") == cycle
            and reset.get("active_nodes") == 3
            and isinstance(reset.get("coordinator_id"), str)
            for cycle, reset in enumerate(resets, 1)
        )
        and len({reset["coordinator_id"] for reset in resets}) == 3,
        "enabled_cold": valid_phase(report, "enabled", "cold"),
        "enabled_warm": valid_phase(report, "enabled", "warm"),
        "disabled_cold": valid_phase(report, "disabled", "cold"),
        "disabled_warm": valid_phase(report, "disabled", "warm"),
        "comparisons": set(comparisons) == {"enabled", "disabled"}
        and all(set(comparisons[variant]) == METRICS for variant in comparisons),
        "observation": observation.get("status")
        in {"benefit_observed", "no_clear_benefit"}
        and isinstance(
            observation.get("enabled_elapsed_time_reduction_percent"), (int, float)
        )
        and isinstance(
            observation.get("disabled_elapsed_time_reduction_percent"), (int, float)
        )
        and isinstance(
            observation.get("net_elapsed_time_reduction_percentage_points"), (int, float)
        ),
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise SystemExit(f"metadata cache evidence failed: {', '.join(failed)}")
    print(
        json.dumps(
            {
                "status": "ready",
                "cache_observation": observation["status"],
                "enabled_elapsed_time_reduction_percent": observation[
                    "enabled_elapsed_time_reduction_percent"
                ],
                "disabled_elapsed_time_reduction_percent": observation[
                    "disabled_elapsed_time_reduction_percent"
                ],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
