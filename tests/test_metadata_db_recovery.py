from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops.metadata_db_recovery import (
    REQUIRED_TABLES,
    STATE_SQL,
    MetadataDbRecoveryError,
    capture_trino_state,
    run_metadata_db_recovery,
    validate_metadata_db_recovery_report,
    write_metadata_db_recovery_report,
)
from lakehouse_ops.trino import TrinoQueryResult, TrinoQueryStats

CONTAINER_A = "a" * 64
CONTAINER_B = "b" * 64
DIGEST = "c" * 64


class FakeClient:
    def __init__(self, *, rows: int = 2, snapshot: str = "42") -> None:
        self.rows = rows
        self.snapshot = snapshot
        self.closed = False

    def query_with_stats(self, sql: str) -> TrinoQueryResult:
        assert sql == STATE_SQL
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


def topology(*, metastore: bool) -> dict[str, Any]:
    return {
        "metastore_container_id": CONTAINER_A,
        "metastore_running": metastore,
        "database_container_id": CONTAINER_B,
        "database_running": True,
    }


def manifest() -> dict[str, Any]:
    return {
        "metastore_schema_version": "4.0.0",
        "entry_count": 3,
        "entries_sha256": DIGEST,
        "required_tables": list(REQUIRED_TABLES),
    }


def backup() -> dict[str, Any]:
    return {
        "format": "postgresql-custom",
        "file_name": "metastore.dump",
        "size_bytes": 1024,
        "sha256": DIGEST,
        "toc_entries": 100,
        "required_tables": list(REQUIRED_TABLES),
    }


def phase(*, metastore: bool = True) -> dict[str, Any]:
    return {
        "catalog": manifest(),
        "trino": {"query_id": "query-42", "row_count": 2, "snapshot_id": "42"},
        "topology": topology(metastore=metastore),
    }


def valid_report() -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "status": "recovered",
        "incident": "postgresql_metastore_catalog_loss",
        "table": "lakehouse_cache_disabled.silver.weather_hourly",
        "collected_at": "2026-08-28T00:00:00+00:00",
        "recovery_duration_seconds": 4.5,
        "backup": backup(),
        "baseline": phase(),
        "stopped": {"topology": topology(metastore=False)},
        "loss": {"core_table_count": 0, "topology": topology(metastore=False)},
        "recovery": phase(),
        "invariants": {
            "backup_verified": True,
            "catalog_loss_observed": True,
            "catalog_manifest_preserved": True,
            "row_count_preserved": True,
            "snapshot_preserved": True,
            "database_container_preserved": True,
            "metastore_service_restored": True,
        },
    }


def test_capture_trino_state_returns_query_linked_snapshot() -> None:
    client = FakeClient()

    assert capture_trino_state(client) == {
        "query_id": "query-42",
        "row_count": 2,
        "snapshot_id": "42",
    }
    assert client.closed is True


def test_run_metadata_db_recovery_restores_and_reconciles_catalog() -> None:
    clients = [FakeClient(), FakeClient()]
    states = iter(
        [
            topology(metastore=True),
            topology(metastore=False),
            topology(metastore=False),
            topology(metastore=True),
        ]
    )
    calls: list[str] = []
    ticks = iter([10.0, 14.25])

    report = run_metadata_db_recovery(
        lambda: clients.pop(0),
        manifest,
        lambda: next(states),
        lambda: calls.append("stop"),
        backup,
        lambda: calls.append("loss"),
        lambda: {"core_table_count": 0},
        lambda: calls.append("restore"),
        lambda: calls.append("start"),
        clock=lambda: datetime(2026, 8, 28, tzinfo=UTC),
        monotonic=lambda: next(ticks),
    )

    assert calls == ["stop", "loss", "restore", "start"]
    assert report["recovery_duration_seconds"] == 4.25
    assert all(report["invariants"].values())


def test_run_metadata_db_recovery_restores_in_finally_after_loss_failure() -> None:
    calls: list[str] = []

    def fail_loss() -> None:
        calls.append("loss")
        raise RuntimeError("loss command failed after changing the schema")

    with pytest.raises(RuntimeError, match="loss command failed"):
        run_metadata_db_recovery(
            lambda: FakeClient(),
            manifest,
            lambda: topology(metastore=not calls),
            lambda: calls.append("stop"),
            backup,
            fail_loss,
            lambda: {"core_table_count": 0},
            lambda: calls.append("restore"),
            lambda: calls.append("start"),
        )

    assert calls == ["stop", "loss", "restore", "start"]


def test_run_metadata_db_recovery_restarts_service_when_backup_is_invalid() -> None:
    calls: list[str] = []
    invalid_backup = backup()
    invalid_backup["size_bytes"] = 0

    with pytest.raises(MetadataDbRecoveryError, match="backup size"):
        run_metadata_db_recovery(
            lambda: FakeClient(),
            manifest,
            lambda: topology(metastore=not calls),
            lambda: calls.append("stop"),
            lambda: invalid_backup,
            lambda: calls.append("loss"),
            lambda: {"core_table_count": 0},
            lambda: calls.append("restore"),
            lambda: calls.append("start"),
        )

    assert calls == ["stop", "start"]


def test_validate_accepts_complete_metadata_restore_evidence() -> None:
    validate_metadata_db_recovery_report(valid_report())


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda report: report["backup"].update(required_tables=[]), "required"),
        (lambda report: report["loss"].update(core_table_count=1), "did not remove"),
        (
            lambda report: report["recovery"]["catalog"].update(entries_sha256="d" * 64),
            "manifest changed",
        ),
        (lambda report: report["recovery"]["trino"].update(row_count=3), "row count"),
        (
            lambda report: report["recovery"]["trino"].update(snapshot_id="43"),
            "snapshot changed",
        ),
        (
            lambda report: report["loss"]["topology"].update(
                database_container_id="d" * 64
            ),
            "database container changed",
        ),
        (lambda report: report.update(invariants={}), "invariants are incomplete"),
    ],
)
def test_validate_rejects_incomplete_recovery(mutate: Any, message: str) -> None:
    report = valid_report()
    mutate(report)

    with pytest.raises(MetadataDbRecoveryError, match=message):
        validate_metadata_db_recovery_report(report)


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda report: report.update(schema_version="2.0"), "schema 1.0"),
        (lambda report: report.update(collected_at=""), "collection time"),
        (lambda report: report.update(recovery_duration_seconds=-1), "duration"),
        (lambda report: report["backup"].update(size_bytes=0), "backup size"),
        (lambda report: report["backup"].update(sha256="bad"), "checksum"),
        (lambda report: report["baseline"].update(catalog=None), "manifest"),
        (
            lambda report: report["baseline"]["catalog"].update(entry_count=0),
            "entry count",
        ),
        (lambda report: report["baseline"]["trino"].update(query_id=""), "query ID"),
        (
            lambda report: report["stopped"]["topology"].update(metastore_running=True),
            "state is unexpected",
        ),
        (
            lambda report: report["recovery"]["topology"].update(database_running=False),
            "database is not running",
        ),
    ],
)
def test_validate_rejects_malformed_evidence(mutate: Any, message: str) -> None:
    report = valid_report()
    mutate(report)

    with pytest.raises(MetadataDbRecoveryError, match=message):
        validate_metadata_db_recovery_report(report)


def test_write_metadata_db_recovery_report_creates_parent(tmp_path: Path) -> None:
    path = tmp_path / "evidence" / "metadata-db.json"

    write_metadata_db_recovery_report(path, valid_report())

    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "recovered"
    assert path.read_text(encoding="utf-8").endswith("\n")
