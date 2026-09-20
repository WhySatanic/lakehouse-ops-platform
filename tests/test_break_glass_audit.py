from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

CHECKER_PATH = Path(__file__).parent / "integration" / "check_break_glass_audit.py"
SPEC = importlib.util.spec_from_file_location("check_break_glass_audit", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


def drill() -> dict[str, object]:
    return {
        "user": "incident-responder",
        "audit_window": {"started_at": "2026-09-20T09:00:00+00:00"},
        "security": {"authentication": "password", "authorization": "ranger"},
    }


def audit(*results: int) -> dict[str, object]:
    return {
        "response": {
            "docs": [
                {
                    "repo": "lakehouse-trino",
                    "enforcer": "ranger-acl",
                    "reqUser": "incident-responder",
                    "result": result,
                    "evtTime": "2026-09-20T09:00:30Z",
                }
                for result in results
            ]
        }
    }


def test_validate_correlates_allowed_and_denied_decisions() -> None:
    report = CHECKER.validate(drill(), audit(1, 0))

    assert report["status"] == "ready"
    assert report["user"] == "incident-responder"
    assert report["allowed"] is True
    assert report["denied"] is True


def test_validate_rejects_missing_denied_decision() -> None:
    with pytest.raises(ValueError, match="denied"):
        CHECKER.validate(drill(), audit(1))


def test_validate_rejects_decisions_before_drill() -> None:
    payload = audit(1, 0)
    for document in payload["response"]["docs"]:
        document["evtTime"] = "2026-09-20T08:59:59Z"

    with pytest.raises(ValueError, match="denied, allowed"):
        CHECKER.validate(drill(), payload)
