from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

CHECKER_PATH = (
    Path(__file__).parent / "integration" / "check_trino_worker_shutdown_audit.py"
)
SPEC = importlib.util.spec_from_file_location(
    "check_trino_worker_shutdown_audit", CHECKER_PATH
)
assert SPEC is not None and SPEC.loader is not None
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


def shutdown_report() -> dict[str, object]:
    return {
        "security": {
            "query_user": "platform_admin",
            "operator_user": "lakehouse-operator",
        },
        "audit_window": {"started_at": "2026-09-18T09:00:00+00:00"},
    }


def ranger_audit(*users: str) -> dict[str, object]:
    return {
        "response": {
            "docs": [
                {
                    "repo": "lakehouse-trino",
                    "enforcer": "ranger-acl",
                    "result": 1,
                    "reqUser": user,
                    "evtTime": "2026-09-18T09:00:30Z",
                }
                for user in users
            ]
        }
    }


def test_validate_correlates_both_shutdown_identities() -> None:
    report = CHECKER.validate(
        shutdown_report(), ranger_audit("platform_admin", "lakehouse-operator")
    )

    assert report == {
        "status": "ready",
        "query_user": "platform_admin",
        "operator_user": "lakehouse-operator",
        "allowed_users": ["lakehouse-operator", "platform_admin"],
    }


def test_validate_rejects_missing_operator_decision() -> None:
    with pytest.raises(ValueError, match="lakehouse-operator"):
        CHECKER.validate(shutdown_report(), ranger_audit("platform_admin"))


def test_validate_rejects_pre_drill_decisions() -> None:
    audit = ranger_audit("platform_admin", "lakehouse-operator")
    for document in audit["response"]["docs"]:
        document["evtTime"] = "2026-09-18T08:59:59Z"

    with pytest.raises(ValueError, match="lakehouse-operator, platform_admin"):
        CHECKER.validate(shutdown_report(), audit)
