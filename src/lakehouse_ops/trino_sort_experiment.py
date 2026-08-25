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


class SortExperimentError(ValueError):
    pass


def capture_sort_order_experiment(
    client: TrinoClient,
    *,
    catalog: str,
    schema: str,
    baseline_table: str,
    sorted_table: str,
    range_start: int,
    range_size: int,
    repetitions: int = 3,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    if repetitions not in {3, 5, 7, 9}:
        raise SortExperimentError("repetitions must be one of 3, 5, 7, or 9")
    if range_start < 0:
        raise SortExperimentError("range start must be non-negative")
    if range_size <= 0:
        raise SortExperimentError("range size must be positive")

    catalog = _identifier(catalog)
    schema = _identifier(schema)
    table_names = {
        "baseline": _identifier(baseline_table),
        "sorted": _identifier(sorted_table),
    }
    tables = {
        variant: _table_state(client, catalog, schema, table)
        for variant, table in table_names.items()
    }
    if tables["baseline"]["record_count"] != tables["sorted"]["record_count"]:
        raise SortExperimentError("experiment tables contain different record counts")
    if tables["baseline"]["partition_count"] != 1 or tables["sorted"][
        "partition_count"
    ] != 1:
        raise SortExperimentError("sort experiment tables must be unpartitioned")
    if tables["baseline"]["sorted_by_event_id"]:
        raise SortExperimentError("baseline table unexpectedly declares an event_id sort order")
    if not tables["sorted"]["sorted_by_event_id"]:
        raise SortExperimentError("sorted table does not declare an event_id sort order")

    range_end = range_start + range_size
    predicate = f"event_id >= {range_start} AND event_id < {range_end}"
    sql_template = (
        "SELECT count(*) AS row_count, sum(event_id) AS event_id_checksum, "
        "max(length(payload)) AS maximum_payload_length FROM {table} WHERE "
        f"{predicate}"
    )
    results = {
        variant: _filtered_result(
            client,
            sql_template.format(table=f"{catalog}.{schema}.{table}"),
        )
        for variant, table in table_names.items()
    }
    if results["baseline"] != results["sorted"]:
        raise SortExperimentError("sort order changed the filtered query result")
    if results["sorted"]["row_count"] != range_size:
        raise SortExperimentError("selective range returned an unexpected row count")

    runs: dict[str, list[dict[str, Any]]] = {"baseline": [], "sorted": []}
    for repetition in range(1, repetitions + 1):
        order = ("baseline", "sorted") if repetition % 2 else ("sorted", "baseline")
        for variant in order:
            qualified = f"{catalog}.{schema}.{table_names[variant]}"
            sql = sql_template.format(table=qualified)
            result = client.query_with_stats(f"EXPLAIN ANALYZE {sql}")
            plan = "\n".join(str(next(iter(row.values()))) for row in result.rows)
            if not plan.strip():
                raise SortExperimentError("EXPLAIN ANALYZE returned an empty plan")
            runs[variant].append(
                {
                    "repetition": repetition,
                    "trino_query_id": result.query_id,
                    "plan_sha256": _digest(plan),
                    "metrics": asdict(result.stats),
                }
            )

    medians = {
        variant: {
            metric: int(statistics.median(run["metrics"][metric] for run in variant_runs))
            for metric in METRICS
        }
        for variant, variant_runs in runs.items()
    }
    comparison = {
        metric: _metric_delta(medians["baseline"][metric], medians["sorted"][metric])
        for metric in METRICS
    }
    if comparison["processed_rows"]["delta"] >= 0:
        raise SortExperimentError("sort order did not reduce processed rows")
    if comparison["physical_input_bytes"]["delta"] >= 0:
        raise SortExperimentError("sort order did not reduce physical input bytes")

    now = clock or (lambda: datetime.now(UTC))
    return {
        "schema_version": "1.1",
        "status": "ready",
        "experiment": "iceberg_sort_order",
        "collected_at": now().astimezone(UTC).isoformat(),
        "engine": "trino",
        "range": {"start": range_start, "end_exclusive": range_end, "size": range_size},
        "predicate_sha256": _digest(predicate),
        "workload": {
            "mode": "explain_analyze",
            "repetitions": repetitions,
            "sql_template_sha256": _digest(sql_template),
        },
        "tables": tables,
        "filtered_result": results["sorted"],
        "runs": runs,
        "medians": medians,
        "comparison": comparison,
        "pruning_evidence": {
            "processed_rows_reduced": True,
            "physical_input_bytes_reduced": True,
            "processed_rows_reduction_percent": _reduction_percent(
                medians["baseline"]["processed_rows"],
                medians["sorted"]["processed_rows"],
            ),
            "physical_input_bytes_reduction_percent": _reduction_percent(
                medians["baseline"]["physical_input_bytes"],
                medians["sorted"]["physical_input_bytes"],
            ),
        },
        "latency_observation": _direction(comparison["wall_time_ms"]["delta"]),
    }


def _table_state(
    client: TrinoClient, catalog: str, schema: str, table: str
) -> dict[str, Any]:
    qualified = f"{catalog}.{schema}.{table}"
    snapshot = client.query(
        f'SELECT snapshot_id FROM {catalog}.{schema}."{table}$snapshots" '
        "ORDER BY committed_at DESC LIMIT 1"
    )
    files = client.query(
        f"SELECT count(*) AS data_file_count, "
        f"coalesce(sum(record_count), 0) AS record_count, "
        f"coalesce(sum(file_size_in_bytes), 0) AS total_size_bytes "
        f'FROM {catalog}.{schema}."{table}$files" WHERE content = 0'
    )
    partitions = client.query(
        f"SELECT count(*) AS partition_count FROM "
        f'{catalog}.{schema}."{table}$partitions"'
    )
    sort_order_rows = client.query(
        f"SELECT value FROM {catalog}.{schema}.\"{table}$properties\" "
        "WHERE key = 'sort-order'"
    )
    if (
        len(snapshot) != 1
        or len(files) != 1
        or len(partitions) != 1
        or len(sort_order_rows) > 1
    ):
        raise SortExperimentError("Iceberg metadata query returned an unexpected row count")
    sort_order = ""
    if sort_order_rows:
        raw_sort_order = sort_order_rows[0].get("value")
        if not isinstance(raw_sort_order, str) or not raw_sort_order.strip():
            raise SortExperimentError("Iceberg sort-order property must be a string")
        sort_order = raw_sort_order.strip()
    return {
        "table": qualified,
        "snapshot_id": _positive_identifier(snapshot[0], "snapshot_id"),
        "data_file_count": _positive_integer(files[0], "data_file_count"),
        "record_count": _positive_integer(files[0], "record_count"),
        "total_size_bytes": _positive_integer(files[0], "total_size_bytes"),
        "partition_count": _positive_integer(partitions[0], "partition_count"),
        "sort_order_sha256": _digest(sort_order),
        "sorted_by_event_id": bool(
            re.match(
                r"^event_id\s+ASC(?:\s+NULLS\s+(?:FIRST|LAST))?(?:\s*,|\s*$)",
                sort_order,
                re.IGNORECASE,
            )
        ),
    }


def _filtered_result(client: TrinoClient, sql: str) -> dict[str, int]:
    rows = client.query(sql)
    if len(rows) != 1:
        raise SortExperimentError("filtered query returned an unexpected row count")
    return {
        "row_count": _positive_integer(rows[0], "row_count"),
        "event_id_checksum": _positive_integer(rows[0], "event_id_checksum"),
        "maximum_payload_length": _positive_integer(rows[0], "maximum_payload_length"),
    }


def _identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise SortExperimentError(f"invalid SQL identifier: {value}")
    return value


def _positive_identifier(value: dict[str, Any], key: str) -> str:
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, (int, str)) or not str(raw):
        raise SortExperimentError(f"{key} must be a non-empty identifier")
    return str(raw)


def _positive_integer(value: dict[str, Any], key: str) -> int:
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        raise SortExperimentError(f"{key} must be a positive integer")
    return raw


def _metric_delta(before: int, after: int) -> dict[str, int | float | None]:
    return {
        "before": before,
        "after": after,
        "delta": after - before,
        "delta_percent": None if before == 0 else round((after - before) * 100 / before, 2),
    }


def _reduction_percent(before: int, after: int) -> float:
    if before <= 0 or after >= before:
        raise SortExperimentError("pruning reduction requires a positive baseline")
    return round((before - after) * 100 / before, 2)


def _direction(delta: int) -> str:
    if delta < 0:
        return "improved"
    if delta > 0:
        return "regressed"
    return "unchanged"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
