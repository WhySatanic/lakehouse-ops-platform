from __future__ import annotations

import hashlib
import re
import statistics
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from lakehouse_ops.trino import TrinoQueryResult

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
CACHE_PROPERTY = "iceberg.metadata-cache.enabled"


class CacheExperimentError(ValueError):
    pass


class QueryClient(Protocol):
    def query(self, sql: str) -> list[dict[str, Any]]: ...

    def query_with_stats(self, sql: str) -> TrinoQueryResult: ...


def validate_cache_catalog_pair(enabled_path: Path, disabled_path: Path) -> dict[str, Any]:
    enabled = _properties(enabled_path)
    disabled = _properties(disabled_path)
    if enabled.get(CACHE_PROPERTY) != "true":
        raise CacheExperimentError("enabled catalog must explicitly enable metadata cache")
    if disabled.get(CACHE_PROPERTY) != "false":
        raise CacheExperimentError("disabled catalog must explicitly disable metadata cache")
    if any(key.startswith("fs.memory-cache.") for key in disabled):
        raise CacheExperimentError("disabled catalog cannot set memory cache tuning")
    enabled_without_toggle = {key: value for key, value in enabled.items() if key != CACHE_PROPERTY}
    disabled_without_toggle = {
        key: value for key, value in disabled.items() if key != CACHE_PROPERTY
    }
    if enabled_without_toggle != disabled_without_toggle:
        raise CacheExperimentError("cache catalogs differ beyond the metadata cache toggle")
    return {
        "enabled_sha256": _digest(enabled_path.read_text(encoding="utf-8")),
        "disabled_sha256": _digest(disabled_path.read_text(encoding="utf-8")),
        "cache_ttl": "default:1h",
        "cache_max_size": "default:2% coordinator heap",
        "cache_max_content_length": "default:15MB",
        "only_difference": CACHE_PROPERTY,
    }


def capture_metadata_cache_experiment(
    client_factory: Callable[[], QueryClient],
    reset_coordinator: Callable[[int], dict[str, Any]],
    *,
    enabled_catalog: str,
    disabled_catalog: str,
    schema: str,
    table: str,
    configuration: dict[str, Any],
    cycles: int = 3,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    if cycles not in {3, 5, 7}:
        raise CacheExperimentError("cycles must be one of 3, 5, or 7")
    catalogs = {
        "enabled": _identifier(enabled_catalog),
        "disabled": _identifier(disabled_catalog),
    }
    schema = _identifier(schema)
    table = _identifier(table)
    sql = {
        variant: (
            "SELECT count(*) AS data_file_count, "
            "coalesce(sum(record_count), 0) AS record_count, "
            "coalesce(sum(file_size_in_bytes), 0) AS total_size_bytes "
            f'FROM {catalog}.{schema}."{table}$files" WHERE content = 0'
        )
        for variant, catalog in catalogs.items()
    }
    runs: dict[str, dict[str, list[dict[str, Any]]]] = {
        "enabled": {"cold": [], "warm": []},
        "disabled": {"cold": [], "warm": []},
    }
    resets: list[dict[str, Any]] = []
    expected_result: dict[str, int] | None = None

    for cycle in range(1, cycles + 1):
        reset = reset_coordinator(cycle)
        if reset.get("cycle") != cycle or reset.get("active_nodes") != 3:
            raise CacheExperimentError("coordinator reset did not restore three active nodes")
        coordinator_id = reset.get("coordinator_id")
        if not isinstance(coordinator_id, str) or not coordinator_id:
            raise CacheExperimentError("coordinator reset did not report an identity")
        if any(previous["coordinator_id"] == coordinator_id for previous in resets):
            raise CacheExperimentError("coordinator identity did not change after restart")
        resets.append(reset)
        order = ("enabled", "disabled") if cycle % 2 else ("disabled", "enabled")
        client = client_factory()
        try:
            for variant in order:
                for phase in ("cold", "warm"):
                    captured = _capture_run(client, sql[variant], cycle)
                    result = captured["result"]
                    if expected_result is None:
                        expected_result = result
                    elif result != expected_result:
                        raise CacheExperimentError("metadata workload results changed between runs")
                    runs[variant][phase].append(captured)
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

    client = client_factory()
    try:
        snapshots = {
            variant: _snapshot_id(client, catalog, schema, table)
            for variant, catalog in catalogs.items()
        }
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()
    if snapshots["enabled"] != snapshots["disabled"]:
        raise CacheExperimentError("cache catalogs resolved different Iceberg snapshots")
    if expected_result is None:
        raise CacheExperimentError("metadata workload produced no result")

    medians = {
        variant: {
            phase: {
                metric: int(
                    statistics.median(run["metrics"][metric] for run in phase_runs)
                )
                for metric in METRICS
            }
            for phase, phase_runs in phases.items()
        }
        for variant, phases in runs.items()
    }
    comparisons = {
        variant: {
            metric: _metric_delta(
                medians[variant]["cold"][metric], medians[variant]["warm"][metric]
            )
            for metric in METRICS
        }
        for variant in catalogs
    }
    enabled_elapsed = _reduction_percent(
        medians["enabled"]["cold"]["elapsed_time_ms"],
        medians["enabled"]["warm"]["elapsed_time_ms"],
    )
    disabled_elapsed = _reduction_percent(
        medians["disabled"]["cold"]["elapsed_time_ms"],
        medians["disabled"]["warm"]["elapsed_time_ms"],
    )
    benefit_observed = enabled_elapsed > 0 and enabled_elapsed > disabled_elapsed
    now = clock or (lambda: datetime.now(UTC))
    return {
        "schema_version": "1.0",
        "status": "ready",
        "experiment": "iceberg_metadata_cache",
        "collected_at": now().astimezone(UTC).isoformat(),
        "engine": "trino",
        "table": f"{enabled_catalog}.{schema}.{table}",
        "snapshot_id": snapshots["enabled"],
        "configuration": configuration,
        "workload": {
            "cycles": cycles,
            "phases": ["cold", "warm"],
            "sql_sha256": _digest(sql["enabled"].replace(enabled_catalog, "{catalog}")),
            "result": expected_result,
        },
        "resets": resets,
        "runs": runs,
        "medians": medians,
        "comparisons": comparisons,
        "cache_observation": {
            "status": "benefit_observed" if benefit_observed else "no_clear_benefit",
            "enabled_elapsed_time_reduction_percent": enabled_elapsed,
            "disabled_elapsed_time_reduction_percent": disabled_elapsed,
            "net_elapsed_time_reduction_percentage_points": round(
                enabled_elapsed - disabled_elapsed, 2
            ),
        },
    }


def _capture_run(client: QueryClient, sql: str, cycle: int) -> dict[str, Any]:
    query = client.query_with_stats(sql)
    if len(query.rows) != 1:
        raise CacheExperimentError("metadata workload returned an unexpected row count")
    result = {
        "data_file_count": _positive_integer(query.rows[0], "data_file_count"),
        "record_count": _positive_integer(query.rows[0], "record_count"),
        "total_size_bytes": _positive_integer(query.rows[0], "total_size_bytes"),
    }
    return {
        "cycle": cycle,
        "trino_query_id": query.query_id,
        "result": result,
        "metrics": asdict(query.stats),
    }


def _snapshot_id(client: QueryClient, catalog: str, schema: str, table: str) -> str:
    rows = client.query(
        f'SELECT snapshot_id FROM {catalog}.{schema}."{table}$snapshots" '
        "ORDER BY committed_at DESC LIMIT 1"
    )
    if len(rows) != 1:
        raise CacheExperimentError("snapshot query returned an unexpected row count")
    value = rows[0].get("snapshot_id")
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value):
        raise CacheExperimentError("snapshot_id must be a non-empty identifier")
    return str(value)


def _properties(path: Path) -> dict[str, str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise CacheExperimentError(f"cannot read catalog configuration: {path}") from error
    properties: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            raise CacheExperimentError(f"invalid catalog property line: {line}")
        key, value = stripped.split("=", 1)
        if not key or key in properties:
            raise CacheExperimentError(f"invalid or duplicate catalog property: {key}")
        properties[key] = value
    return properties


def _identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise CacheExperimentError(f"invalid SQL identifier: {value}")
    return value


def _positive_integer(value: dict[str, Any], key: str) -> int:
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        raise CacheExperimentError(f"{key} must be a positive integer")
    return raw


def _metric_delta(before: int, after: int) -> dict[str, int | float | None]:
    return {
        "before": before,
        "after": after,
        "delta": after - before,
        "delta_percent": None if before == 0 else round((after - before) * 100 / before, 2),
    }


def _reduction_percent(before: int, after: int) -> float:
    if before <= 0:
        return 0.0
    return round((before - after) * 100 / before, 2)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
