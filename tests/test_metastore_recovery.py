from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops.metastore_recovery import (
    MetastoreRecoveryError,
    run_metastore_recovery,
    validate_metastore_recovery_report,
    write_metastore_recovery_report,
)
from lakehouse_ops.trino import TrinoQueryError, TrinoQueryResult, TrinoQueryStats

CONTAINER_A = "a" * 64
CONTAINER_B = "b" * 64


class FakeClient:
    def __init__(self, *, fail: bool = False, rows: int = 2, snapshot: str = "42") -> None:
        self.fail = fail
        self.rows = rows
        self.snapshot = snapshot
        self.closed = False

    def query_with_stats(self, sql: str) -> TrinoQueryResult:
        assert "lakehouse_cache_disabled.silver.weather_hourly" in sql
        if self.fail:
            raise TrinoQueryError("HIVE_METASTORE_ERROR: metastore unavailable")
        return TrinoQueryResult(
            query_id=f"query-{self.snapshot}",
            rows=({"row_count": self.rows, "snapshot_id": self.snapshot},),
            stats=TrinoQueryStats(
                state="FINISHED",
                elapsed_time_ms=10,
                wall_time_ms=10,
                cpu_time_ms=5,
                processed_rows=self.rows,
                processed_bytes=100,
                physical_input_bytes=50,
                peak_memory_bytes=100,
                spilled_bytes=0,
            ),
        )

    def close(self) -> None:
        self.closed = True


def topology(*, metastore: bool, database_id: str = CONTAINER_B) -> dict[str, Any]:
    return {
        "metastore_container_id": CONTAINER_A,
        "metastore_running": metastore,
        "database_container_id": database_id,
        "database_running": True,
    }


def valid_report() -> dict[str, Any]:
    phase = {
        "query_id": "query-42",
        "row_count": 2,
        "snapshot_id": "42",
        "topology": topology(metastore=True),
    }
    return {
        "schema_version": "1.0",
        "status": "recovered",
        "incident": "hive_metastore_unavailable",
        "table": "lakehouse_cache_disabled.silver.weather_hourly",
        "collected_at": "2026-08-27T00:00:00+00:00",
        "outage_duration_seconds": 3.0,
        "baseline": dict(phase),
        "outage": {
            "metastore_query_failed": True,
            "error_type": "TrinoQueryError",
            "error": "HIVE_METASTORE_ERROR: metastore unavailable",
            "topology": topology(metastore=False),
        },
        "recovery": dict(phase),
        "invariants": {
            "row_count_preserved": True,
            "snapshot_preserved": True,
            "metadata_db_container_preserved": True,
        },
    }


def test_run_metastore_recovery_restores_service_and_preserves_state() -> None:
    clients = [FakeClient(), FakeClient(fail=True), FakeClient()]
    states = iter(
        [
            topology(metastore=True),
            topology(metastore=False),
            topology(metastore=True),
        ]
    )
    calls: list[str] = []
    ticks = iter([10.0, 13.25])

    report = run_metastore_recovery(
        lambda: clients.pop(0),
        lambda: next(states),
        lambda: calls.append("stop"),
        lambda: calls.append("start"),
        clock=lambda: datetime(2026, 8, 27, tzinfo=UTC),
        monotonic=lambda: next(ticks),
    )

    assert calls == ["stop", "start"]
    assert report["outage_duration_seconds"] == 3.25
    assert report["outage"]["error_type"] == "TrinoQueryError"
    assert report["invariants"] == {
        "row_count_preserved": True,
        "snapshot_preserved": True,
        "metadata_db_container_preserved": True,
    }


def test_run_metastore_recovery_restores_service_when_outage_query_succeeds() -> None:
    clients = [FakeClient(), FakeClient()]
    states = iter([topology(metastore=True), topology(metastore=False)])
    calls: list[str] = []

    with pytest.raises(MetastoreRecoveryError, match="query succeeded"):
        run_metastore_recovery(
            lambda: clients.pop(0),
            lambda: next(states),
            lambda: calls.append("stop"),
            lambda: calls.append("start"),
        )

    assert calls == ["stop", "start"]


def test_validate_accepts_complete_report() -> None:
    validate_metastore_recovery_report(valid_report())


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda report: report["recovery"].update(row_count=3), "row count changed"),
        (lambda report: report["recovery"].update(snapshot_id="43"), "snapshot changed"),
        (
            lambda report: report["outage"]["topology"].update(
                database_container_id="c" * 64
            ),
            "database container changed",
        ),
        (
            lambda report: report["outage"].update(metastore_query_failed=False),
            "outage query did not fail",
        ),
    ],
)
def test_validate_rejects_incomplete_recovery(mutate: Any, message: str) -> None:
    report = valid_report()
    mutate(report)

    with pytest.raises(MetastoreRecoveryError, match=message):
        validate_metastore_recovery_report(report)


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda report: report.update(schema_version="2.0"), "schema 1.0"),
        (lambda report: report.update(incident="other"), "incident or table"),
        (lambda report: report.update(collected_at=""), "collection time"),
        (lambda report: report.update(outage_duration_seconds=-1), "outage duration"),
        (lambda report: report.update(outage=None), "outage evidence"),
        (lambda report: report["outage"].update(error_type=""), "error type"),
        (lambda report: report["outage"].update(error=""), "outage error is missing"),
        (lambda report: report["baseline"].update(query_id=""), "query ID"),
        (lambda report: report["baseline"].update(row_count=0), "row count is invalid"),
        (lambda report: report["baseline"].update(snapshot_id="bad"), "snapshot ID"),
        (
            lambda report: report["outage"]["topology"].update(database_running=False),
            "metadata database stopped",
        ),
        (
            lambda report: report["recovery"]["topology"].update(
                metastore_container_id="bad"
            ),
            "metastore_container_id is invalid",
        ),
        (lambda report: report.update(invariants={}), "invariants are incomplete"),
    ],
)
def test_validate_rejects_malformed_evidence(mutate: Any, message: str) -> None:
    report = valid_report()
    mutate(report)

    with pytest.raises(MetastoreRecoveryError, match=message):
        validate_metastore_recovery_report(report)


def test_write_metastore_recovery_report_creates_parent(tmp_path: Path) -> None:
    path = tmp_path / "evidence" / "metastore.json"

    write_metastore_recovery_report(path, valid_report())

    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "recovered"
    assert path.read_text(encoding="utf-8").endswith("\n")
