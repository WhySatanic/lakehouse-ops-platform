from __future__ import annotations

import hashlib
import re
import statistics
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from lakehouse_ops.trino import TrinoClient

IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
METRICS = (
    "elapsed_time_ms",
    "wall_time_ms",
    "cpu_time_ms",
    "processed_rows",
    "processed_bytes",
    "physical_input_bytes",
    "peak_memory_bytes",
    "spilled_bytes",
)


class TrinoExperimentError(ValueError):
    pass


def capture_compaction_phase(
    client: TrinoClient,
    *,
    catalog: str,
    schema: str,
    table: str,
    phase: str,
    repetitions: int = 3,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    if phase not in {"before", "after"}:
        raise TrinoExperimentError("phase must be before or after")
    if repetitions not in {3, 5, 7, 9}:
        raise TrinoExperimentError("repetitions must be one of 3, 5, 7, or 9")
    qualified = ".".join(_identifier(value) for value in (catalog, schema, table))
    snapshot = client.query(
        f'SELECT snapshot_id FROM {catalog}.{schema}."{table}$snapshots" '
        "ORDER BY committed_at DESC LIMIT 1"
    )
    files = client.query(
        f'SELECT count(*) AS data_file_count, '
        f'coalesce(sum(record_count), 0) AS record_count, '
        f'coalesce(sum(file_size_in_bytes), 0) AS total_size_bytes '
        f'FROM {catalog}.{schema}."{table}$files" WHERE content = 0'
    )
    if len(snapshot) != 1 or len(files) != 1:
        raise TrinoExperimentError("Iceberg metadata query returned an unexpected row count")
    snapshot_id = _positive_identifier(snapshot[0], "snapshot_id")
    file_layout = {
        "data_file_count": _positive_integer(files[0], "data_file_count"),
        "record_count": _positive_integer(files[0], "record_count"),
        "total_size_bytes": _positive_integer(files[0], "total_size_bytes"),
    }
    sql = (
        f"SELECT count(*) AS row_count, sum(event_id) AS event_id_checksum, "
        f"max(length(payload)) AS maximum_payload_length FROM {qualified}"
    )
    runs: list[dict[str, Any]] = []
    for repetition in range(1, repetitions + 1):
        result = client.query_with_stats(f"EXPLAIN ANALYZE {sql}")
        plan = "\n".join(str(next(iter(row.values()))) for row in result.rows)
        if not plan.strip():
            raise TrinoExperimentError("EXPLAIN ANALYZE returned an empty plan")
        runs.append(
            {
                "repetition": repetition,
                "trino_query_id": result.query_id,
                "plan_sha256": _digest(plan),
                "metrics": asdict(result.stats),
            }
        )
    now = clock or (lambda: datetime.now(UTC))
    return {
        "schema_version": "1.0",
        "status": "ready",
        "experiment": "iceberg_data_file_compaction",
        "phase": phase,
        "collected_at": now().astimezone(UTC).isoformat(),
        "engine": "trino",
        "table": qualified,
        "snapshot_id": snapshot_id,
        "file_layout": file_layout,
        "workload": {
            "sql_sha256": _digest(sql),
            "mode": "explain_analyze",
            "repetitions": repetitions,
        },
        "runs": runs,
        "medians": {
            metric: int(statistics.median(run["metrics"][metric] for run in runs))
            for metric in METRICS
        },
    }


def compare_compaction_phases(
    before: dict[str, Any], after: dict[str, Any], execution: dict[str, Any]
) -> dict[str, Any]:
    _validate_phase(before, "before")
    _validate_phase(after, "after")
    if before["table"] != after["table"]:
        raise TrinoExperimentError("benchmark phases target different tables")
    if before["workload"] != after["workload"]:
        raise TrinoExperimentError("benchmark phases use different workloads")
    if execution.get("status") != "succeeded" or execution.get("action_type") != (
        "rewrite_data_files"
    ):
        raise TrinoExperimentError("execution report is not a successful data-file rewrite")
    execution_before = execution.get("before", {})
    execution_after = execution.get("after", {})
    if not isinstance(execution_before, dict) or not isinstance(execution_after, dict):
        raise TrinoExperimentError("execution report has invalid table state")
    if str(execution_before.get("snapshot_id")) != before["snapshot_id"]:
        raise TrinoExperimentError("before benchmark snapshot does not match execution")
    if str(execution_after.get("snapshot_id")) != after["snapshot_id"]:
        raise TrinoExperimentError("after benchmark snapshot does not match execution")
    before_layout = before["file_layout"]
    after_layout = after["file_layout"]
    if before_layout["data_file_count"] != execution_before.get("data_file_count"):
        raise TrinoExperimentError("before file count does not match execution")
    if after_layout["data_file_count"] != execution_after.get("data_file_count"):
        raise TrinoExperimentError("after file count does not match execution")
    if before_layout["record_count"] != execution_before.get("record_count"):
        raise TrinoExperimentError("before record count does not match execution")
    if after_layout["record_count"] != execution_after.get("record_count"):
        raise TrinoExperimentError("after record count does not match execution")
    if before_layout["record_count"] != after_layout["record_count"]:
        raise TrinoExperimentError("record count changed across compaction")
    if after_layout["data_file_count"] >= before_layout["data_file_count"]:
        raise TrinoExperimentError("compaction did not reduce the data-file count")
    comparison = {
        metric: _metric_delta(before["medians"][metric], after["medians"][metric])
        for metric in METRICS
    }
    files_before = before_layout["data_file_count"]
    files_after = after_layout["data_file_count"]
    return {
        "schema_version": "1.0",
        "status": "ready",
        "experiment": "iceberg_data_file_compaction",
        "table": before["table"],
        "snapshots": {"before": before["snapshot_id"], "after": after["snapshot_id"]},
        "file_layout": {
            "before": before_layout,
            "after": after_layout,
            "file_count_reduction": files_before - files_after,
            "file_count_reduction_percent": round(
                (files_before - files_after) * 100 / files_before, 2
            ),
        },
        "workload": before["workload"],
        "medians": {"before": before["medians"], "after": after["medians"]},
        "comparison": comparison,
        "latency_observation": _direction(comparison["wall_time_ms"]["delta"]),
    }


def _validate_phase(report: dict[str, Any], phase: str) -> None:
    if report.get("schema_version") != "1.0" or report.get("status") != "ready":
        raise TrinoExperimentError(f"{phase} benchmark report is not ready")
    if report.get("experiment") != "iceberg_data_file_compaction":
        raise TrinoExperimentError(f"{phase} benchmark has the wrong experiment")
    if report.get("phase") != phase:
        raise TrinoExperimentError(f"expected {phase} benchmark phase")
    if not isinstance(report.get("table"), str) or not report["table"]:
        raise TrinoExperimentError(f"{phase} benchmark table is invalid")
    if not isinstance(report.get("snapshot_id"), str) or not report["snapshot_id"]:
        raise TrinoExperimentError(f"{phase} benchmark snapshot is invalid")
    layout = report.get("file_layout")
    if not isinstance(layout, dict) or not all(
        isinstance(layout.get(key), int)
        and not isinstance(layout.get(key), bool)
        and layout[key] > 0
        for key in ("data_file_count", "record_count", "total_size_bytes")
    ):
        raise TrinoExperimentError(f"{phase} benchmark file layout is invalid")
    workload = report.get("workload")
    if (
        not isinstance(workload, dict)
        or workload.get("mode") != "explain_analyze"
        or workload.get("repetitions") not in {3, 5, 7, 9}
        or not isinstance(workload.get("sql_sha256"), str)
        or len(workload["sql_sha256"]) != 64
    ):
        raise TrinoExperimentError(f"{phase} benchmark workload is invalid")
    if set(report.get("medians", {})) != set(METRICS):
        raise TrinoExperimentError(f"{phase} benchmark medians are incomplete")
    if not all(
        isinstance(value, int) and not isinstance(value, bool) and value >= 0
        for value in report["medians"].values()
    ):
        raise TrinoExperimentError(f"{phase} benchmark medians are invalid")


def _metric_delta(before: int, after: int) -> dict[str, int | float | None]:
    return {
        "before": before,
        "after": after,
        "delta": after - before,
        "delta_percent": None if before == 0 else round((after - before) * 100 / before, 2),
    }


def _direction(delta: int) -> str:
    if delta < 0:
        return "improved"
    if delta > 0:
        return "regressed"
    return "unchanged"


def _identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise TrinoExperimentError(f"invalid SQL identifier: {value}")
    return value


def _positive_identifier(value: dict[str, Any], key: str) -> str:
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, (int, str)) or not str(raw):
        raise TrinoExperimentError(f"{key} must be a non-empty identifier")
    return str(raw)


def _positive_integer(value: dict[str, Any], key: str) -> int:
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        raise TrinoExperimentError(f"{key} must be a positive integer")
    return raw


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
