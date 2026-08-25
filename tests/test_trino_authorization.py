from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
CHECKER_PATH = ROOT / "tests" / "integration" / "check_trino_authorization.py"
SPEC = importlib.util.spec_from_file_location("check_trino_authorization", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


def valid_report() -> dict[str, object]:
    cases = []
    for case_id, result in CHECKER.EXPECTED_CASES.items():
        cases.append(
            {
                "id": case_id,
                "user": "test-user",
                "expectation": "allow" if result == "allowed" else "deny",
                "result": result,
            }
        )
    return {
        "schema_version": "1.0",
        "status": "succeeded",
        "policy": {
            "engine": "trino",
            "mode": "file",
            "default": "deny",
            "authentication_enforced": False,
        },
        "cases": cases,
    }


def test_validate_accepts_complete_authorization_evidence() -> None:
    assert CHECKER.validate(valid_report()) == []


def test_validate_rejects_missing_denial_and_false_security_claim() -> None:
    report = valid_report()
    report["policy"]["authentication_enforced"] = True
    report["cases"].pop()

    assert CHECKER.validate(report) == [
        "policy.authentication_enforced",
        "cases.coverage",
    ]


def test_policy_has_explicit_catalog_table_and_system_fallback_denials() -> None:
    policy = json.loads(
        (ROOT / "infra" / "trino" / "access-control-rules.json").read_text(
            encoding="utf-8"
        )
    )

    assert policy["catalogs"][-1] == {"catalog": ".*", "allow": "none"}
    assert policy["tables"][-1]["privileges"] == []
    assert policy["system_information"][-1]["allow"] == []
    assert policy["system_session_properties"][-1]["allow"] is False
    assert policy["catalog_session_properties"][-1]["allow"] is False
