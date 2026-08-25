from __future__ import annotations

import importlib.util
from pathlib import Path

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


def test_validate_accepts_complete_worker_shutdown_evidence() -> None:
    assert CHECKER.validate(valid_report()) == []


def test_validate_rejects_missing_continuity_and_live_endpoint() -> None:
    report = valid_report()
    report["shutdown"]["endpoint_stopped"] = False
    report["continuity"]["silver_rows"] = 0

    assert CHECKER.validate(report) == ["endpoint_stopped", "silver_rows"]
