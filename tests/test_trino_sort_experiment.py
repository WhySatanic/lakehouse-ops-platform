from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lakehouse_ops.trino import TrinoQueryResult, TrinoQueryStats
from lakehouse_ops.trino_sort_experiment import (
    SortExperimentError,
    _direction,
    _reduction_percent,
    capture_sort_order_experiment,
)


class FakeTrinoClient:
    def __init__(
        self,
        *,
        record_mismatch: bool = False,
        result_mismatch: bool = False,
        pruning: bool = True,
        physical_pruning: bool | None = None,
        missing_sort_order: bool = False,
        baseline_sort_order: bool = False,
        partitioned: bool = False,
        target_count_mismatch: bool = False,
        metadata_empty: bool = False,
        filtered_empty: bool = False,
        invalid_snapshot: bool = False,
        invalid_file_count: bool = False,
        empty_plan: bool = False,
    ) -> None:
        self.record_mismatch = record_mismatch
        self.result_mismatch = result_mismatch
        self.pruning = pruning
        self.physical_pruning = pruning if physical_pruning is None else physical_pruning
        self.missing_sort_order = missing_sort_order
        self.baseline_sort_order = baseline_sort_order
        self.partitioned = partitioned
        self.target_count_mismatch = target_count_mismatch
        self.metadata_empty = metadata_empty
        self.filtered_empty = filtered_empty
        self.invalid_snapshot = invalid_snapshot
        self.invalid_file_count = invalid_file_count
        self.empty_plan = empty_plan
        self.queries: list[str] = []

    def query(self, sql: str) -> list[dict[str, int | str]]:
        self.queries.append(sql)
        sorted_table = self._sorted(sql)
        if self.metadata_empty and "$snapshots" in sql:
            return []
        if "$snapshots" in sql:
            snapshot_id = True if self.invalid_snapshot else (22 if sorted_table else 21)
            return [{"snapshot_id": snapshot_id}]
        if "$files" in sql:
            records = 65_535 if sorted_table and self.record_mismatch else 65_536
            return [
                {
                    "data_file_count": 0 if self.invalid_file_count else 16,
                    "record_count": records,
                    "total_size_bytes": 12_000,
                }
            ]
        if "$partitions" in sql:
            return [{"partition_count": 2 if self.partitioned else 1}]
        if "$properties" in sql:
            sort_order = False
            if (sorted_table and not self.missing_sort_order) or (
                not sorted_table and self.baseline_sort_order
            ):
                sort_order = True
            return (
                [{"value": "event_id ASC NULLS FIRST"}]
                if sort_order
                else []
            )
        if self.filtered_empty:
            return []
        checksum = 99_999 if sorted_table and self.result_mismatch else 3_848_128
        return [
            {
                "row_count": 127 if self.target_count_mismatch else 128,
                "event_id_checksum": checksum,
                "maximum_payload_length": 512,
            }
        ]

    def query_with_stats(self, sql: str) -> TrinoQueryResult:
        self.queries.append(sql)
        sorted_table = self._sorted(sql)
        repetition = len(
            [
                query
                for query in self.queries
                if query.startswith("EXPLAIN") and self._sorted(query) == sorted_table
            ]
        )
        rows = () if self.empty_plan else ({"Query Plan": f"Scan {sorted_table}"},)
        processed_rows = 4_096 if sorted_table and self.pruning else 65_536
        physical_input = 10_000 if sorted_table and self.physical_pruning else 160_000
        return TrinoQueryResult(
            query_id=f"query-{sorted_table}-{repetition}",
            rows=rows,
            stats=TrinoQueryStats(
                state="FINISHED",
                elapsed_time_ms=(20 if sorted_table else 40) + repetition,
                wall_time_ms=(10 if sorted_table else 30) + repetition,
                cpu_time_ms=(5 if sorted_table else 15) + repetition,
                processed_rows=processed_rows,
                processed_bytes=physical_input * 2,
                physical_input_bytes=physical_input,
                peak_memory_bytes=512,
                spilled_bytes=0,
            ),
        )

    @staticmethod
    def _sorted(sql: str) -> bool:
        return "sort_ordered" in sql


def capture(client: FakeTrinoClient, **kwargs: object) -> dict[str, object]:
    arguments = {
        "catalog": "lakehouse",
        "schema": "ops",
        "baseline_table": "sort_baseline",
        "sorted_table": "sort_ordered",
        "range_start": 30_000,
        "range_size": 128,
        "repetitions": 3,
        "clock": lambda: datetime(2026, 8, 25, 8, 0, tzinfo=UTC),
    }
    arguments.update(kwargs)
    return capture_sort_order_experiment(client, **arguments)


def test_capture_sort_order_experiment_records_reduction() -> None:
    client = FakeTrinoClient()
    report = capture(client)

    assert report["schema_version"] == "1.1"
    assert report["tables"]["baseline"]["snapshot_id"] == "21"
    assert report["tables"]["sorted"]["sorted_by_event_id"] is True
    assert report["filtered_result"]["row_count"] == 128
    assert report["medians"]["baseline"]["processed_rows"] == 65_536
    assert report["medians"]["sorted"]["processed_rows"] == 4_096
    assert report["comparison"]["physical_input_bytes"]["delta"] == -150_000
    assert report["pruning_evidence"]["processed_rows_reduction_percent"] == 93.75
    assert report["latency_observation"] == "improved"
    assert report["collected_at"] == "2026-08-25T08:00:00+00:00"
    assert len(report["runs"]["baseline"]) == 3
    assert len(report["runs"]["sorted"]) == 3
    assert any("$properties" in query for query in client.queries)
    assert not any(query.startswith("SHOW CREATE TABLE") for query in client.queries)


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"repetitions": 2}, "repetitions must"),
        ({"range_start": -1}, "non-negative"),
        ({"range_size": 0}, "positive"),
        ({"sorted_table": "bad-name"}, "invalid SQL identifier"),
    ],
)
def test_capture_sort_order_experiment_rejects_invalid_contract(
    kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises(SortExperimentError, match=message):
        capture(FakeTrinoClient(), **kwargs)


@pytest.mark.parametrize(
    "client, message",
    [
        (FakeTrinoClient(record_mismatch=True), "different record counts"),
        (FakeTrinoClient(result_mismatch=True), "changed the filtered query result"),
        (FakeTrinoClient(missing_sort_order=True), "does not declare"),
        (FakeTrinoClient(baseline_sort_order=True), "baseline table unexpectedly"),
        (FakeTrinoClient(partitioned=True), "must be unpartitioned"),
        (FakeTrinoClient(target_count_mismatch=True), "unexpected row count"),
        (FakeTrinoClient(metadata_empty=True), "metadata query returned"),
        (FakeTrinoClient(filtered_empty=True), "filtered query returned"),
        (FakeTrinoClient(invalid_snapshot=True), "non-empty identifier"),
        (FakeTrinoClient(invalid_file_count=True), "positive integer"),
        (FakeTrinoClient(pruning=False), "did not reduce processed rows"),
        (
            FakeTrinoClient(physical_pruning=False),
            "did not reduce physical input bytes",
        ),
        (FakeTrinoClient(empty_plan=True), "empty plan"),
    ],
)
def test_capture_sort_order_experiment_rejects_invalid_evidence(
    client: FakeTrinoClient, message: str
) -> None:
    with pytest.raises(SortExperimentError, match=message):
        capture(client)


def test_sort_experiment_metric_helpers_cover_non_improving_cases() -> None:
    assert _direction(1) == "regressed"
    assert _direction(0) == "unchanged"
    with pytest.raises(SortExperimentError, match="positive baseline"):
        _reduction_percent(0, 0)
