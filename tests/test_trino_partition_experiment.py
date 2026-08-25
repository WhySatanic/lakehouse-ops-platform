from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lakehouse_ops.trino import TrinoQueryResult, TrinoQueryStats
from lakehouse_ops.trino_partition_experiment import (
    PartitionExperimentError,
    capture_partition_pruning_experiment,
)


class FakeTrinoClient:
    def __init__(
        self,
        *,
        record_mismatch: bool = False,
        result_mismatch: bool = False,
        pruning: bool = True,
        physical_pruning: bool | None = None,
        empty_plan: bool = False,
    ) -> None:
        self.record_mismatch = record_mismatch
        self.result_mismatch = result_mismatch
        self.pruning = pruning
        self.physical_pruning = pruning if physical_pruning is None else physical_pruning
        self.empty_plan = empty_plan
        self.queries: list[str] = []

    def query(self, sql: str) -> list[dict[str, int]]:
        self.queries.append(sql)
        partitioned = self._partitioned(sql)
        if "$snapshots" in sql:
            return [{"snapshot_id": 12 if partitioned else 11}]
        if "$files" in sql:
            records = 65_535 if partitioned and self.record_mismatch else 65_536
            return [
                {
                    "data_file_count": 32 if partitioned else 2,
                    "record_count": records,
                    "total_size_bytes": 12_000 if partitioned else 10_000,
                }
            ]
        if "$partitions" in sql:
            return [{"partition_count": 32 if partitioned else 1}]
        checksum = 99_999 if partitioned and self.result_mismatch else 65_535_000
        return [
            {
                "row_count": 2_048,
                "event_id_checksum": checksum,
                "maximum_payload_length": 81,
            }
        ]

    def query_with_stats(self, sql: str) -> TrinoQueryResult:
        self.queries.append(sql)
        partitioned = self._partitioned(sql)
        repetition = len(
            [
                query
                for query in self.queries
                if query.startswith("EXPLAIN")
                and self._partitioned(query) == partitioned
            ]
        )
        rows = () if self.empty_plan else ({"Query Plan": f"Scan {partitioned}"},)
        processed_rows = 2_048 if partitioned and self.pruning else 32_768
        physical_input = 500 if partitioned and self.physical_pruning else 5_000
        return TrinoQueryResult(
            query_id=f"query-{partitioned}-{repetition}",
            rows=rows,
            stats=TrinoQueryStats(
                state="FINISHED",
                elapsed_time_ms=(20 if partitioned else 40) + repetition,
                wall_time_ms=(10 if partitioned else 30) + repetition,
                cpu_time_ms=(5 if partitioned else 15) + repetition,
                processed_rows=processed_rows,
                processed_bytes=physical_input * 2,
                physical_input_bytes=physical_input,
                peak_memory_bytes=512,
                spilled_bytes=0,
            ),
        )

    @staticmethod
    def _partitioned(sql: str) -> bool:
        return "pruning_partitioned" in sql and "pruning_unpartitioned" not in sql


def capture(client: FakeTrinoClient, **kwargs: object) -> dict[str, object]:
    arguments = {
        "catalog": "lakehouse",
        "schema": "ops",
        "unpartitioned_table": "pruning_unpartitioned",
        "partitioned_table": "pruning_partitioned",
        "target_day": "2026-01-16",
        "repetitions": 3,
        "clock": lambda: datetime(2026, 8, 25, 7, 0, tzinfo=UTC),
    }
    arguments.update(kwargs)
    return capture_partition_pruning_experiment(client, **arguments)


def test_capture_partition_pruning_experiment_records_reduction() -> None:
    report = capture(FakeTrinoClient())

    assert report["tables"]["unpartitioned"]["snapshot_id"] == "11"
    assert report["tables"]["partitioned"]["partition_count"] == 32
    assert report["filtered_result"]["row_count"] == 2_048
    assert report["medians"]["unpartitioned"]["processed_rows"] == 32_768
    assert report["medians"]["partitioned"]["processed_rows"] == 2_048
    assert report["comparison"]["physical_input_bytes"]["delta"] == -4_500
    assert report["pruning_evidence"]["processed_rows_reduction_percent"] == 93.75
    assert report["latency_observation"] == "improved"
    assert report["collected_at"] == "2026-08-25T07:00:00+00:00"
    assert len(report["runs"]["unpartitioned"]) == 3
    assert len(report["runs"]["partitioned"]) == 3


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"repetitions": 2}, "repetitions must"),
        ({"target_day": "16-01-2026"}, "ISO format"),
        ({"partitioned_table": "bad-name"}, "invalid SQL identifier"),
    ],
)
def test_capture_partition_pruning_experiment_rejects_invalid_contract(
    kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises(PartitionExperimentError, match=message):
        capture(FakeTrinoClient(), **kwargs)


@pytest.mark.parametrize(
    "client, message",
    [
        (FakeTrinoClient(record_mismatch=True), "different record counts"),
        (FakeTrinoClient(result_mismatch=True), "changed the filtered query result"),
        (FakeTrinoClient(pruning=False), "did not reduce processed rows"),
        (
            FakeTrinoClient(physical_pruning=False),
            "did not reduce physical input bytes",
        ),
        (FakeTrinoClient(empty_plan=True), "empty plan"),
    ],
)
def test_capture_partition_pruning_experiment_rejects_invalid_evidence(
    client: FakeTrinoClient, message: str
) -> None:
    with pytest.raises(PartitionExperimentError, match=message):
        capture(client)
