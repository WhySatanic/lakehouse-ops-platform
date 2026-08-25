from __future__ import annotations

import hashlib
import re
import statistics
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
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


class PartitionExperimentError(ValueError):
    pass


def capture_partition_pruning_experiment(
    client: TrinoClient,
    *,
    catalog: str,
    schema: str,
    unpartitioned_table: str,
    partitioned_table: str,
    target_day: str,
    repetitions: int = 3,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    if repetitions not in {3, 5, 7, 9}:
        raise PartitionExperimentError("repetitions must be one of 3, 5, 7, or 9")
    try:
        day = date.fromisoformat(target_day)
    except ValueError as error:
        raise PartitionExperimentError("target day must use ISO format YYYY-MM-DD") from error
    if day.isoformat() != target_day:
        raise PartitionExperimentError("target day must use ISO format YYYY-MM-DD")

    catalog = _identifier(catalog)
    schema = _identifier(schema)
    table_names = {
        "unpartitioned": _identifier(unpartitioned_table),
        "partitioned": _identifier(partitioned_table),
    }
    tables = {
        variant: _table_state(client, catalog, schema, table)
        for variant, table in table_names.items()
    }
    if tables["unpartitioned"]["record_count"] != tables["partitioned"][
        "record_count"
    ]:
        raise PartitionExperimentError("experiment tables contain different record counts")
    if tables["unpartitioned"]["partition_count"] != 1:
        raise PartitionExperimentError("unpartitioned table must expose one partition")
    if tables["partitioned"]["partition_count"] <= 1:
        raise PartitionExperimentError("partitioned table must expose multiple partitions")

    next_day = day + timedelta(days=1)
    predicate = (
        f"event_ts >= TIMESTAMP '{day.isoformat()} 00:00:00' "
        f"AND event_ts < TIMESTAMP '{next_day.isoformat()} 00:00:00'"
    )
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
    if results["unpartitioned"] != results["partitioned"]:
        raise PartitionExperimentError("partitioning changed the filtered query result")
    if results["partitioned"]["row_count"] <= 0:
        raise PartitionExperimentError("target day returned no rows")

    runs: dict[str, list[dict[str, Any]]] = {
        "unpartitioned": [],
        "partitioned": [],
    }
    for repetition in range(1, repetitions + 1):
        order = (
            ("unpartitioned", "partitioned")
            if repetition % 2
            else ("partitioned", "unpartitioned")
        )
        for variant in order:
            qualified = f"{catalog}.{schema}.{table_names[variant]}"
            sql = sql_template.format(table=qualified)
            result = client.query_with_stats(f"EXPLAIN ANALYZE {sql}")
            plan = "\n".join(str(next(iter(row.values()))) for row in result.rows)
            if not plan.strip():
                raise PartitionExperimentError("EXPLAIN ANALYZE returned an empty plan")
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
        metric: _metric_delta(
            medians["unpartitioned"][metric], medians["partitioned"][metric]
        )
        for metric in METRICS
    }
    if comparison["processed_rows"]["delta"] >= 0:
        raise PartitionExperimentError("partition pruning did not reduce processed rows")
    if comparison["physical_input_bytes"]["delta"] >= 0:
        raise PartitionExperimentError("partition pruning did not reduce physical input bytes")

    now = clock or (lambda: datetime.now(UTC))
    return {
        "schema_version": "1.0",
        "status": "ready",
        "experiment": "iceberg_partition_pruning",
        "collected_at": now().astimezone(UTC).isoformat(),
        "engine": "trino",
        "target_day": day.isoformat(),
        "predicate_sha256": _digest(predicate),
        "workload": {
            "mode": "explain_analyze",
            "repetitions": repetitions,
            "sql_template_sha256": _digest(sql_template),
        },
        "tables": tables,
        "filtered_result": results["partitioned"],
        "runs": runs,
        "medians": medians,
        "comparison": comparison,
        "pruning_evidence": {
            "processed_rows_reduced": True,
            "physical_input_bytes_reduced": True,
            "processed_rows_reduction_percent": _reduction_percent(
                medians["unpartitioned"]["processed_rows"],
                medians["partitioned"]["processed_rows"],
            ),
            "physical_input_bytes_reduction_percent": _reduction_percent(
                medians["unpartitioned"]["physical_input_bytes"],
                medians["partitioned"]["physical_input_bytes"],
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
        f'SELECT count(*) AS data_file_count, '
        f'coalesce(sum(record_count), 0) AS record_count, '
        f'coalesce(sum(file_size_in_bytes), 0) AS total_size_bytes '
        f'FROM {catalog}.{schema}."{table}$files" WHERE content = 0'
    )
    partitions = client.query(
        f'SELECT count(*) AS partition_count FROM '
        f'{catalog}.{schema}."{table}$partitions"'
    )
    if len(snapshot) != 1 or len(files) != 1 or len(partitions) != 1:
        raise PartitionExperimentError("Iceberg metadata query returned an unexpected row count")
    return {
        "table": qualified,
        "snapshot_id": _positive_identifier(snapshot[0], "snapshot_id"),
        "data_file_count": _positive_integer(files[0], "data_file_count"),
        "record_count": _positive_integer(files[0], "record_count"),
        "total_size_bytes": _positive_integer(files[0], "total_size_bytes"),
        "partition_count": _positive_integer(partitions[0], "partition_count"),
    }


def _filtered_result(client: TrinoClient, sql: str) -> dict[str, int]:
    rows = client.query(sql)
    if len(rows) != 1:
        raise PartitionExperimentError("filtered query returned an unexpected row count")
    return {
        "row_count": _positive_integer(rows[0], "row_count"),
        "event_id_checksum": _positive_integer(rows[0], "event_id_checksum"),
        "maximum_payload_length": _positive_integer(rows[0], "maximum_payload_length"),
    }


def _identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise PartitionExperimentError(f"invalid SQL identifier: {value}")
    return value


def _positive_identifier(value: dict[str, Any], key: str) -> str:
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, (int, str)) or not str(raw):
        raise PartitionExperimentError(f"{key} must be a non-empty identifier")
    return str(raw)


def _positive_integer(value: dict[str, Any], key: str) -> int:
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        raise PartitionExperimentError(f"{key} must be a positive integer")
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
        raise PartitionExperimentError("pruning reduction requires a positive baseline")
    return round((before - after) * 100 / before, 2)


def _direction(delta: int) -> str:
    if delta < 0:
        return "improved"
    if delta > 0:
        return "regressed"
    return "unchanged"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
