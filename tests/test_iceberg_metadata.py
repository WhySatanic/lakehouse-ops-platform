from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from lakehouse_ops.iceberg.metadata import (
    IcebergMetadataCollector,
    MetadataContractError,
)


class FakeExecutor:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def query(self, sql: str) -> list[dict[str, Any]]:
        self.queries.append(sql)
        if "$snapshots" in sql:
            return [
                {
                    "snapshot_count": "3",
                    "current_snapshot_id": "8750000000000000001",
                    "current_committed_at": "2026-08-18 13:00:00.000 UTC",
                    "current_operation": "overwrite",
                }
            ]
        if "$files" in sql:
            return [
                {
                    "file_count": 2,
                    "record_count": 100,
                    "total_size_bytes": 4096,
                    "min_size_bytes": 1024,
                    "max_size_bytes": 3072,
                    "delete_file_count": 0,
                }
            ]
        if "$manifests" in sql:
            return [
                {
                    "manifest_count": 1,
                    "total_size_bytes": 800,
                    "added_files": 2,
                    "existing_files": 0,
                    "deleted_files": 0,
                }
            ]
        if "$partitions" in sql:
            return [
                {
                    "partition_count": 2,
                    "record_count": 100,
                    "file_count": 2,
                    "total_size_bytes": 4096,
                }
            ]
        raise AssertionError(sql)


def test_collect_normalizes_iceberg_metadata() -> None:
    executor = FakeExecutor()
    collector = IcebergMetadataCollector(
        executor, clock=lambda: datetime(2026, 8, 18, 13, 30, tzinfo=UTC)
    )

    report = collector.collect("lakehouse", "silver", "weather_hourly").as_dict()

    assert report == {
        "schema_version": "1.0",
        "status": "ready",
        "collected_at": "2026-08-18T13:30:00+00:00",
        "table": "lakehouse.silver.weather_hourly",
        "snapshots": {
            "count": 3,
            "current_id": "8750000000000000001",
            "committed_at": "2026-08-18 13:00:00.000 UTC",
            "operation": "overwrite",
        },
        "files": {
            "count": 2,
            "records": 100,
            "total_size_bytes": 4096,
            "min_size_bytes": 1024,
            "max_size_bytes": 3072,
            "delete_file_count": 0,
        },
        "manifests": {
            "count": 1,
            "total_size_bytes": 800,
            "added_files": 2,
            "existing_files": 0,
            "deleted_files": 0,
        },
        "partitions": {
            "count": 2,
            "records": 100,
            "files": 2,
            "total_size_bytes": 4096,
        },
    }
    assert len(executor.queries) == 4


def test_collect_quotes_identifiers() -> None:
    executor = FakeExecutor()

    IcebergMetadataCollector(executor).collect("lakehouse", "ops-data", 'table"name')

    assert '"lakehouse"."ops-data"."table""name$snapshots"' in executor.queries[0]


def test_collect_rejects_table_without_snapshot() -> None:
    class EmptySnapshotExecutor(FakeExecutor):
        def query(self, sql: str) -> list[dict[str, Any]]:
            if "$snapshots" in sql:
                return [
                    {
                        "snapshot_count": 0,
                        "current_snapshot_id": None,
                        "current_committed_at": None,
                        "current_operation": None,
                    }
                ]
            return super().query(sql)

    with pytest.raises(MetadataContractError, match="no current snapshot"):
        IcebergMetadataCollector(EmptySnapshotExecutor()).collect("c", "s", "t")
