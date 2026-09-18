from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

CHECKER_PATH = Path(__file__).parent / "integration" / "check_trino_worker_shutdown.py"
SPEC = importlib.util.spec_from_file_location("check_trino_worker_shutdown", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


def valid_report() -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "status": "succeeded",
        "topology": {
            "active_nodes_before": 3,
            "active_workers_before": 2,
            "active_nodes_after": 2,
            "active_workers_after": 1,
        },
        "shutdown": {
            "target_node_id": "lakehouse-worker-2",
            "state_before": "ACTIVE",
            "state_after_request": "SHUTTING_DOWN",
            "target_registered_after": False,
            "endpoint_stopped": True,
            "grace_period_seconds": 5,
        },
        "continuity": {
            "silver_rows": 2,
            "snapshot_metadata_readable": True,
        },
    }


def secure_report() -> dict[str, object]:
    report = valid_report()
    report["security"] = {
        "authentication": "password",
        "authorization": "ranger",
        "operator_user": "lakehouse-operator",
        "query_user": "platform_admin",
        "tls_verified": True,
        "transport": "https",
    }
    report["audit_window"] = {
        "started_at": "2026-09-18T09:00:00+00:00",
        "completed_at": "2026-09-18T09:01:00+00:00",
    }
    report["topology"].update(
        active_nodes_restored=3,
        active_workers_restored=2,
        target_registered_restored=True,
    )
    report["shutdown"].update(
        target_node_id="lakehouse-secure-worker-1",
        target_service="trino-secure-worker",
        container_id_before="a" * 64,
        container_running_after=False,
        container_id_restored="b" * 64,
        container_running_restored=True,
    )
    fingerprint = {
        "query_id": "20260918_000001_00001_abcd1",
        "row_count": 2,
        "data_checksum": "0123456789abcdef",
        "snapshot_id": "123456789",
    }
    report["continuity"].update(
        baseline=fingerprint.copy(),
        drained=fingerprint.copy(),
        restored=fingerprint.copy(),
    )
    report["in_flight_query"] = {
        "query_id": "20260918_000002_00002_abcd2",
        "target_task_observed": True,
        "target_task_count": 1,
        "terminal_state": "FINISHED",
    }
    report["invariants"] = {
        "zero_query_interruption": True,
        "data_preserved": True,
        "worker_capacity_restored": True,
    }
    return report


def test_validate_accepts_complete_worker_shutdown_evidence() -> None:
    assert CHECKER.validate(valid_report()) == []


def test_validate_rejects_missing_continuity_and_live_endpoint() -> None:
    report = valid_report()
    report["shutdown"]["endpoint_stopped"] = False
    report["continuity"]["silver_rows"] = 0

    assert CHECKER.validate(report) == ["endpoint_stopped", "silver_rows"]


def test_validate_accepts_authenticated_zero_interruption_drain() -> None:
    assert CHECKER.validate(secure_report()) == []


@pytest.mark.parametrize(
    ("section", "field", "value", "expected"),
    [
        ("security", "tls_verified", False, "security"),
        ("in_flight_query", "terminal_state", "FAILED", "in_flight_finished"),
        ("invariants", "data_preserved", False, "data_preserved"),
    ],
)
def test_validate_rejects_untrusted_or_interrupted_secure_drain(
    section: str, field: str, value: object, expected: str
) -> None:
    report = secure_report()
    report[section][field] = value

    assert expected in CHECKER.validate(report)


def test_authenticated_shutdown_is_wired_into_ranger_ci() -> None:
    root = Path(__file__).parents[1]
    runner = (root / "tests/integration/exercise_trino_worker_shutdown.py").read_text(
        encoding="utf-8"
    )
    workflow = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "wait_for_target_task" in runner
    assert 'query_result.get("terminal_state") != "FINISHED"' in runner
    assert "Exercise authenticated graceful worker drain" in workflow
    assert "check_trino_worker_shutdown_audit.py" in workflow
