from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

import pytest

from lakehouse_ops.trino import TrinoQueryResult, TrinoQueryStats
from lakehouse_ops.trino_experiment import (
    TrinoExperimentError,
    capture_compaction_phase,
    compare_compaction_phases,
)


class FakeTrinoClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def query(self, sql: str) -> list[dict[str, int]]:
        self.queries.append(sql)
        if "$snapshots" in sql:
            return [{"snapshot_id": 42}]
        return [
            {
                "data_file_count": 4,
                "record_count": 100_000,
                "total_size_bytes": 4096,
            }
        ]

    def query_with_stats(self, sql: str) -> TrinoQueryResult:
        self.queries.append(sql)
        repetition = len([query for query in self.queries if query.startswith("EXPLAIN")])
        return TrinoQueryResult(
            query_id=f"query-{repetition}",
            rows=({"Query Plan": f"Fragment {repetition}\nScan"},),
            stats=TrinoQueryStats(
                state="FINISHED",
                elapsed_time_ms=20 + repetition,
                wall_time_ms=10 + repetition,
                cpu_time_ms=5 + repetition,
                processed_rows=100_000,
                processed_bytes=2048,
                physical_input_bytes=1024,
                peak_memory_bytes=512,
                spilled_bytes=0,
            ),
        )


def test_capture_compaction_phase_records_repeated_medians() -> None:
    client = FakeTrinoClient()

    report = capture_compaction_phase(
        client,
        catalog="lakehouse",
        schema="ops",
        table="maintenance_fixture",
        phase="before",
        repetitions=3,
        clock=lambda: datetime(2026, 8, 25, 6, 0, tzinfo=UTC),
    )

    assert report["snapshot_id"] == "42"
    assert report["file_layout"] == {
        "data_file_count": 4,
        "record_count": 100_000,
        "total_size_bytes": 4096,
    }
    assert report["medians"]["wall_time_ms"] == 12
    assert report["workload"]["repetitions"] == 3
    assert len(report["runs"]) == 3
    assert report["collected_at"] == "2026-08-25T06:00:00+00:00"


def phase_report(phase: str, *, snapshot: str, files: int, wall: int) -> dict[str, object]:
    medians = {
        "elapsed_time_ms": wall + 5,
        "wall_time_ms": wall,
        "cpu_time_ms": wall // 2,
        "processed_rows": 100_000,
        "processed_bytes": 2048,
        "physical_input_bytes": 1024,
        "peak_memory_bytes": 512,
        "spilled_bytes": 0,
    }
    return {
        "schema_version": "1.0",
        "status": "ready",
        "experiment": "iceberg_data_file_compaction",
        "phase": phase,
        "table": "lakehouse.ops.maintenance_fixture",
        "snapshot_id": snapshot,
        "file_layout": {
            "data_file_count": files,
            "record_count": 100_000,
            "total_size_bytes": 4096,
        },
        "workload": {
            "sql_sha256": "a" * 64,
            "mode": "explain_analyze",
            "repetitions": 3,
        },
        "medians": medians,
    }


def execution_report() -> dict[str, object]:
    return {
        "status": "succeeded",
        "action_type": "rewrite_data_files",
        "before": {
            "snapshot_id": "41",
            "data_file_count": 4,
            "record_count": 100_000,
        },
        "after": {
            "snapshot_id": "42",
            "data_file_count": 1,
            "record_count": 100_000,
        },
    }


def test_compare_compaction_phases_links_measurement_to_execution() -> None:
    report = compare_compaction_phases(
        phase_report("before", snapshot="41", files=4, wall=20),
        phase_report("after", snapshot="42", files=1, wall=12),
        execution_report(),
    )

    assert report["file_layout"]["file_count_reduction"] == 3
    assert report["file_layout"]["file_count_reduction_percent"] == 75.0
    assert report["comparison"]["wall_time_ms"] == {
        "before": 20,
        "after": 12,
        "delta": -8,
        "delta_percent": -40.0,
    }
    assert report["latency_observation"] == "improved"


@pytest.mark.parametrize(
    "before_wall, after_wall, observation",
    [(20, 20, "unchanged"), (20, 25, "regressed")],
)
def test_compare_compaction_phases_classifies_latency_direction(
    before_wall: int, after_wall: int, observation: str
) -> None:
    report = compare_compaction_phases(
        phase_report("before", snapshot="41", files=4, wall=before_wall),
        phase_report("after", snapshot="42", files=1, wall=after_wall),
        execution_report(),
    )

    assert report["latency_observation"] == observation


def test_compare_compaction_phases_rejects_unrelated_snapshot() -> None:
    execution = execution_report()
    execution["after"]["snapshot_id"] = "different"

    with pytest.raises(TrinoExperimentError, match="after benchmark snapshot"):
        compare_compaction_phases(
            phase_report("before", snapshot="41", files=4, wall=20),
            phase_report("after", snapshot="42", files=1, wall=12),
            execution,
        )


@pytest.mark.parametrize(
    "path, value, message",
    [
        (("status",), "failed", "not ready"),
        (("experiment",), "other", "wrong experiment"),
        (("phase",), "after", "expected before"),
        (("table",), "", "table is invalid"),
        (("snapshot_id",), None, "snapshot is invalid"),
        (("file_layout", "record_count"), 0, "file layout is invalid"),
        (("workload", "mode"), "query", "workload is invalid"),
        (("medians", "cpu_time_ms"), None, "medians are invalid"),
    ],
)
def test_compare_compaction_phases_rejects_invalid_phase_report(
    path: tuple[str, ...], value: object, message: str
) -> None:
    before = phase_report("before", snapshot="41", files=4, wall=20)
    target = before
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value

    with pytest.raises(TrinoExperimentError, match=message):
        compare_compaction_phases(
            before,
            phase_report("after", snapshot="42", files=1, wall=12),
            execution_report(),
        )


def test_compare_compaction_phases_requires_all_medians() -> None:
    before = phase_report("before", snapshot="41", files=4, wall=20)
    before["medians"].pop("spilled_bytes")

    with pytest.raises(TrinoExperimentError, match="medians are incomplete"):
        compare_compaction_phases(
            before,
            phase_report("after", snapshot="42", files=1, wall=12),
            execution_report(),
        )


@pytest.mark.parametrize(
    "mutation, message",
    [
        (("action_type", "rewrite_manifests"), "successful data-file rewrite"),
        (("before", []), "invalid table state"),
        (("before.data_file_count", 3), "before file count"),
        (("after.record_count", 99_999), "after record count"),
    ],
)
def test_compare_compaction_phases_rejects_inconsistent_execution(
    mutation: tuple[str, object], message: str
) -> None:
    execution = deepcopy(execution_report())
    path, value = mutation
    if "." in path:
        section, key = path.split(".")
        execution[section][key] = value
    else:
        execution[path] = value

    with pytest.raises(TrinoExperimentError, match=message):
        compare_compaction_phases(
            phase_report("before", snapshot="41", files=4, wall=20),
            phase_report("after", snapshot="42", files=1, wall=12),
            execution,
        )


def test_compare_compaction_phases_requires_file_reduction() -> None:
    execution = execution_report()
    execution["after"]["data_file_count"] = 4

    with pytest.raises(TrinoExperimentError, match="did not reduce"):
        compare_compaction_phases(
            phase_report("before", snapshot="41", files=4, wall=20),
            phase_report("after", snapshot="42", files=4, wall=12),
            execution,
        )


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"phase": "during"}, "phase must"),
        ({"repetitions": 2}, "repetitions must"),
        ({"table": "bad-name"}, "invalid SQL identifier"),
    ],
)
def test_capture_compaction_phase_rejects_invalid_contract(
    kwargs: dict[str, object], message: str
) -> None:
    arguments = {
        "catalog": "lakehouse",
        "schema": "ops",
        "table": "maintenance_fixture",
        "phase": "before",
        "repetitions": 3,
    }
    arguments.update(kwargs)

    with pytest.raises(TrinoExperimentError, match=message):
        capture_compaction_phase(FakeTrinoClient(), **arguments)
