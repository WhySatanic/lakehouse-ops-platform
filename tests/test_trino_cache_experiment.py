from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakehouse_ops.trino import TrinoQueryResult, TrinoQueryStats
from lakehouse_ops.trino_cache_experiment import (
    CacheExperimentError,
    _properties,
    capture_metadata_cache_experiment,
    validate_cache_catalog_pair,
)

BASE_PROPERTIES = """connector.name=iceberg
iceberg.catalog.type=hive_metastore
iceberg.security=READ_ONLY
iceberg.metadata-cache.enabled={enabled}
"""


class FakeClient:
    def __init__(
        self,
        *,
        result_mismatch: bool = False,
        snapshot_mismatch: bool = False,
        invalid_result: bool = False,
        empty_rows: bool = False,
        empty_snapshot: bool = False,
        invalid_snapshot: bool = False,
    ) -> None:
        self.result_mismatch = result_mismatch
        self.snapshot_mismatch = snapshot_mismatch
        self.invalid_result = invalid_result
        self.empty_rows = empty_rows
        self.empty_snapshot = empty_snapshot
        self.invalid_snapshot = invalid_snapshot
        self.calls: dict[str, int] = {"enabled": 0, "disabled": 0}
        self.closed = False

    def query_with_stats(self, sql: str) -> TrinoQueryResult:
        variant = "disabled" if "cache_disabled" in sql else "enabled"
        self.calls[variant] += 1
        call = self.calls[variant]
        warm = call % 2 == 0
        rows = () if self.empty_rows else (
            {
                "data_file_count": 0 if self.invalid_result else 32,
                "record_count": 65_535
                if self.result_mismatch and variant == "disabled"
                else 65_536,
                "total_size_bytes": 2_000_000,
            },
        )
        wall = 20 if variant == "enabled" and warm else 100
        if variant == "disabled" and warm:
            wall = 90
        return TrinoQueryResult(
            query_id=f"query-{variant}-{call}",
            rows=rows,
            stats=TrinoQueryStats(
                state="FINISHED",
                elapsed_time_ms=wall + 5,
                wall_time_ms=wall,
                cpu_time_ms=10,
                processed_rows=32,
                processed_bytes=2_000,
                physical_input_bytes=1_000,
                peak_memory_bytes=512,
                spilled_bytes=0,
            ),
        )

    def query(self, sql: str) -> list[dict[str, int | bool]]:
        if self.empty_snapshot:
            return []
        if self.invalid_snapshot:
            return [{"snapshot_id": True}]
        snapshot = 42
        if self.snapshot_mismatch and "cache_disabled" in sql:
            snapshot = 43
        return [{"snapshot_id": snapshot}]

    def close(self) -> None:
        self.closed = True


def configuration() -> dict[str, str]:
    return {
        "enabled_sha256": "a" * 64,
        "disabled_sha256": "b" * 64,
        "only_difference": "iceberg.metadata-cache.enabled",
    }


def capture(client: FakeClient, **kwargs: object) -> dict[str, object]:
    arguments = {
        "enabled_catalog": "lakehouse",
        "disabled_catalog": "lakehouse_cache_disabled",
        "schema": "ops",
        "table": "pruning_partitioned",
        "configuration": configuration(),
        "cycles": 3,
        "clock": lambda: datetime(2026, 8, 25, 9, 0, tzinfo=UTC),
    }
    arguments.update(kwargs)
    return capture_metadata_cache_experiment(
        lambda: client,
        lambda cycle: {"cycle": cycle, "active_nodes": 3, "coordinator_id": f"c{cycle}"},
        **arguments,
    )


def test_validate_cache_catalog_pair_accepts_one_toggle(tmp_path: Path) -> None:
    enabled = tmp_path / "enabled.properties"
    disabled = tmp_path / "disabled.properties"
    enabled.write_text(BASE_PROPERTIES.format(enabled="true"), encoding="utf-8")
    disabled.write_text(BASE_PROPERTIES.format(enabled="false"), encoding="utf-8")

    report = validate_cache_catalog_pair(enabled, disabled)

    assert report["cache_ttl"] == "default:1h"
    assert report["cache_max_size"] == "default:2% coordinator heap"
    assert report["only_difference"] == "iceberg.metadata-cache.enabled"
    assert len(report["enabled_sha256"]) == 64


@pytest.mark.parametrize(
    "enabled_value, disabled_value, extra, message",
    [
        ("false", "false", "", "explicitly enable"),
        ("true", "true", "", "explicitly disable"),
        ("true", "false", "other.property=value\n", "differ beyond"),
    ],
)
def test_validate_cache_catalog_pair_rejects_invalid_pair(
    tmp_path: Path,
    enabled_value: str,
    disabled_value: str,
    extra: str,
    message: str,
) -> None:
    enabled = tmp_path / "enabled.properties"
    disabled = tmp_path / "disabled.properties"
    enabled.write_text(BASE_PROPERTIES.format(enabled=enabled_value) + extra, encoding="utf-8")
    disabled.write_text(BASE_PROPERTIES.format(enabled=disabled_value), encoding="utf-8")

    with pytest.raises(CacheExperimentError, match=message):
        validate_cache_catalog_pair(enabled, disabled)


def test_properties_rejects_invalid_files(tmp_path: Path) -> None:
    with pytest.raises(CacheExperimentError, match="cannot read"):
        _properties(tmp_path / "missing.properties")
    invalid = tmp_path / "invalid.properties"
    invalid.write_text("invalid\n", encoding="utf-8")
    with pytest.raises(CacheExperimentError, match="invalid catalog property line"):
        _properties(invalid)
    invalid.write_text("key=1\nkey=2\n", encoding="utf-8")
    with pytest.raises(CacheExperimentError, match="duplicate"):
        _properties(invalid)


def test_validate_cache_catalog_pair_rejects_disabled_cache_tuning(
    tmp_path: Path,
) -> None:
    enabled = tmp_path / "enabled.properties"
    disabled = tmp_path / "disabled.properties"
    enabled.write_text(BASE_PROPERTIES.format(enabled="true"), encoding="utf-8")
    disabled.write_text(
        BASE_PROPERTIES.format(enabled="false") + "fs.memory-cache.ttl=1h\n",
        encoding="utf-8",
    )

    with pytest.raises(CacheExperimentError, match="disabled catalog cannot set"):
        validate_cache_catalog_pair(enabled, disabled)


def test_capture_metadata_cache_experiment_reports_controlled_observation() -> None:
    client = FakeClient()

    report = capture(client)

    assert report["snapshot_id"] == "42"
    assert report["workload"]["cycles"] == 3
    assert report["workload"]["result"]["record_count"] == 65_536
    assert report["medians"]["enabled"]["cold"]["wall_time_ms"] == 100
    assert report["medians"]["enabled"]["warm"]["wall_time_ms"] == 20
    assert report["medians"]["disabled"]["warm"]["wall_time_ms"] == 90
    assert report["cache_observation"]["status"] == "benefit_observed"
    assert report["cache_observation"]["enabled_elapsed_time_reduction_percent"] == 76.19
    assert report["cache_observation"]["net_elapsed_time_reduction_percentage_points"] == 66.67
    assert report["collected_at"] == "2026-08-25T09:00:00+00:00"
    assert client.closed is True


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"cycles": 2}, "cycles must"),
        ({"enabled_catalog": "bad-name"}, "invalid SQL identifier"),
    ],
)
def test_capture_metadata_cache_experiment_rejects_invalid_contract(
    kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises(CacheExperimentError, match=message):
        capture(FakeClient(), **kwargs)


@pytest.mark.parametrize(
    "client, message",
    [
        (FakeClient(result_mismatch=True), "results changed"),
        (FakeClient(snapshot_mismatch=True), "different Iceberg snapshots"),
        (FakeClient(invalid_result=True), "positive integer"),
        (FakeClient(empty_rows=True), "unexpected row count"),
        (FakeClient(empty_snapshot=True), "snapshot query returned"),
        (FakeClient(invalid_snapshot=True), "non-empty identifier"),
    ],
)
def test_capture_metadata_cache_experiment_rejects_invalid_evidence(
    client: FakeClient, message: str
) -> None:
    with pytest.raises(CacheExperimentError, match=message):
        capture(client)


def test_capture_metadata_cache_experiment_rejects_incomplete_reset() -> None:
    with pytest.raises(CacheExperimentError, match="three active nodes"):
        capture_metadata_cache_experiment(
            lambda: FakeClient(),
            lambda cycle: {"cycle": cycle, "active_nodes": 2},
            enabled_catalog="lakehouse",
            disabled_catalog="lakehouse_cache_disabled",
            schema="ops",
            table="pruning_partitioned",
            configuration=configuration(),
        )


@pytest.mark.parametrize(
    "reset, message",
    [
        (lambda cycle: {"cycle": cycle, "active_nodes": 3}, "report an identity"),
        (
            lambda cycle: {
                "cycle": cycle,
                "active_nodes": 3,
                "coordinator_id": "unchanged",
            },
            "identity did not change",
        ),
    ],
)
def test_capture_metadata_cache_experiment_rejects_unproven_restart(
    reset: Callable[[int], dict[str, object]], message: str
) -> None:
    with pytest.raises(CacheExperimentError, match=message):
        capture_metadata_cache_experiment(
            lambda: FakeClient(),
            reset,
            enabled_catalog="lakehouse",
            disabled_catalog="lakehouse_cache_disabled",
            schema="ops",
            table="pruning_partitioned",
            configuration=configuration(),
        )
