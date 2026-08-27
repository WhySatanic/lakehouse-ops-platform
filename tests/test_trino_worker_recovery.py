from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops.trino import TrinoQueryResult, TrinoQueryStats
from lakehouse_ops.trino_worker_recovery import (
    STATE_SQL,
    TrinoWorkerRecoveryError,
    capture_data_state,
    validate_trino_worker_recovery_report,
    write_trino_worker_recovery_report,
)

CONTAINER_A = "a" * 64
CONTAINER_B = "b" * 64


class FakeClient:
    def query_with_stats(self, sql: str) -> TrinoQueryResult:
        assert sql == STATE_SQL
        return TrinoQueryResult(
            query_id="capture-query",
            rows=(
                {
                    "row_count": 2,
                    "data_checksum": "ed7782190f239a7f",
                    "snapshot_id": "42",
                },
            ),
            stats=TrinoQueryStats(
                state="FINISHED",
                elapsed_time_ms=10,
                wall_time_ms=10,
                cpu_time_ms=5,
                processed_rows=2,
                processed_bytes=100,
                physical_input_bytes=50,
                peak_memory_bytes=100,
                spilled_bytes=0,
            ),
        )


def topology(nodes: int, workers: int, target: bool) -> dict[str, Any]:
    return {
        "active_nodes": nodes,
        "active_workers": workers,
        "target_registered": target,
    }


def data_phase(nodes: int, workers: int, target: bool) -> dict[str, Any]:
    return {
        "query_id": "capture-query",
        "row_count": 2,
        "data_checksum": "ed7782190f239a7f",
        "snapshot_id": "42",
        "topology": topology(nodes, workers, target),
    }


def valid_report() -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "status": "recovered",
        "incident": "trino_worker_abrupt_loss",
        "table": "lakehouse.silver.weather_hourly",
        "policy": {
            "retry_policy": "NONE",
            "remote_task_max_error_duration": "15s",
        },
        "collected_at": "2026-08-27T00:00:00+00:00",
        "worker_loss_duration_seconds": 8.5,
        "baseline": data_phase(3, 2, True),
        "in_flight_query": {
            "query_id": "long-query",
            "target_task_observed": True,
            "target_task_count": 1,
            "terminal_state": "FAILED",
            "error_name": "TOO_MANY_REQUESTS_FAILED",
            "failure_message": "worker failed",
        },
        "loss": {
            "target_service": "trino-worker",
            "target_node_id": "lakehouse-worker-1",
            "signal": "SIGKILL",
            "container_id_before": CONTAINER_A,
            "restart_policy_before": "on-failure",
            "container_running_after": False,
            "topology": topology(2, 1, False),
        },
        "degraded_recovery": data_phase(2, 1, False),
        "restored": {
            **data_phase(3, 2, True),
            "container": {
                "id": CONTAINER_B,
                "running": True,
                "restart_policy": "on-failure",
            },
        },
        "invariants": {
            "in_flight_failure_observed": True,
            "degraded_retry_succeeded": True,
            "row_count_preserved": True,
            "data_checksum_preserved": True,
            "snapshot_preserved": True,
            "worker_capacity_restored": True,
            "restart_policy_restored": True,
        },
    }


def test_capture_data_state_returns_query_linked_fingerprint() -> None:
    assert capture_data_state(FakeClient()) == {
        "query_id": "capture-query",
        "row_count": 2,
        "data_checksum": "ed7782190f239a7f",
        "snapshot_id": "42",
    }


def test_validate_accepts_complete_worker_recovery_evidence() -> None:
    validate_trino_worker_recovery_report(valid_report())


@pytest.mark.parametrize(
    "mutate, message",
    [
        (
            lambda report: report["in_flight_query"].update(
                target_task_observed=False
            ),
            "target worker task",
        ),
        (
            lambda report: report["in_flight_query"].update(
                terminal_state="FINISHED"
            ),
            "did not fail",
        ),
        (
            lambda report: report["in_flight_query"].update(error_name="USER_CANCELED"),
            "remote-task failure",
        ),
        (lambda report: report["loss"].update(signal="SIGTERM"), "not SIGKILL"),
        (
            lambda report: report["loss"].update(target_service="trino-worker-2"),
            "target service",
        ),
        (
            lambda report: report["degraded_recovery"].update(row_count=3),
            "Iceberg data changed",
        ),
        (
            lambda report: report["restored"]["container"].update(id=CONTAINER_A),
            "not recreated",
        ),
        (lambda report: report.update(invariants={}), "invariants are incomplete"),
    ],
)
def test_validate_rejects_incomplete_recovery(mutate: Any, message: str) -> None:
    report = valid_report()
    mutate(report)

    with pytest.raises(TrinoWorkerRecoveryError, match=message):
        validate_trino_worker_recovery_report(report)


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda report: report.update(schema_version="2.0"), "schema 1.0"),
        (lambda report: report.update(collected_at=""), "collection time"),
        (lambda report: report.update(policy={}), "policy"),
        (
            lambda report: report.update(worker_loss_duration_seconds=-1),
            "duration",
        ),
        (lambda report: report["baseline"].update(query_id=""), "query ID"),
        (lambda report: report["baseline"].update(row_count=0), "row count"),
        (
            lambda report: report["baseline"].update(data_checksum="not-hex"),
            "checksum",
        ),
        (lambda report: report["baseline"].update(snapshot_id="bad"), "snapshot ID"),
        (
            lambda report: report["loss"].update(container_id_before="bad"),
            "container.*ID",
        ),
        (
            lambda report: report["restored"].update(
                topology=topology(2, 1, False)
            ),
            "restored active node",
        ),
    ],
)
def test_validate_rejects_malformed_evidence(mutate: Any, message: str) -> None:
    report = valid_report()
    mutate(report)

    with pytest.raises(TrinoWorkerRecoveryError, match=message):
        validate_trino_worker_recovery_report(report)


def test_write_worker_recovery_report_creates_parent(tmp_path: Path) -> None:
    path = tmp_path / "evidence" / "worker.json"

    write_trino_worker_recovery_report(path, valid_report())

    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "recovered"
    assert path.read_text(encoding="utf-8").endswith("\n")
