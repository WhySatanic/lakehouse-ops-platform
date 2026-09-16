from __future__ import annotations

import hashlib
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
        "authentication": {
            "anonymous_request": "not_tested",
            "incorrect_password": "not_tested",
        },
        "transformations": {
            "analytics_engineer_visible_rows": 2,
            "analytics_engineer_visible_checksums": 2,
            "platform_admin_visible_checksums": 2,
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


def test_validate_accepts_authenticated_ranger_evidence() -> None:
    report = valid_report()
    report["policy"]["mode"] = "ranger"
    report["policy"]["authentication_enforced"] = True
    report["authentication"] = {
        "anonymous_request": "denied",
        "incorrect_password": "denied",
    }
    report["transformations"] = {
        "analytics_engineer_visible_rows": 1,
        "analytics_engineer_visible_checksums": 0,
        "platform_admin_visible_checksums": 2,
    }

    assert (
        CHECKER.validate(
            report,
            expected_mode="ranger",
            authentication_enforced=True,
        )
        == []
    )


def test_validate_rejects_unproven_authentication_boundary() -> None:
    report = valid_report()
    report["policy"]["authentication_enforced"] = True

    assert CHECKER.validate(report, authentication_enforced=True) == ["authentication"]


def test_secure_profile_requires_tls_password_authentication_and_ranger() -> None:
    config = (ROOT / "infra" / "trino" / "secure" / "config.properties").read_text(
        encoding="utf-8"
    )
    authenticator = (
        ROOT / "infra" / "trino" / "secure" / "password-authenticator.properties"
    ).read_text(encoding="utf-8")
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    secure_service = compose[
        compose.index("  trino-secure-coordinator:") : compose.index(
            "  trino-secure-authorization-check:"
        )
    ]

    assert "http-server.https.enabled=true" in config
    assert "http-server.authentication.type=PASSWORD" in config
    assert "internal-communication.shared-secret=${ENV:TRINO_INTERNAL_SHARED_SECRET}" in config
    assert "http-server.authentication.allow-insecure-over-http" not in config
    assert "password-authenticator.name=file" in authenticator
    assert 'profiles: ["secure-query"]' in secure_service
    assert "ranger-access-control.properties" in secure_service
    assert "TRINO_INTERNAL_SHARED_SECRET" in secure_service
    assert "TRINO_TLS_KEYSTORE_PASSWORD" in secure_service
    assert (
        "docker compose --profile security --profile catalog --profile secure-query \\\n"
        "            run --rm trino-secure-authorization-check"
        in workflow
    )


def test_development_password_file_contains_valid_pbkdf2_hashes() -> None:
    password_file = ROOT / "infra" / "trino" / "secure" / "password.db"
    records = [line.split(":") for line in password_file.read_text().splitlines()]
    expected_users = {
        "platform_admin",
        "data_engineer",
        "analytics_engineer",
        "analyst",
        "service_ingest",
        "untrusted_user",
        "lakehouse-operator",
    }

    assert {record[0] for record in records} == expected_users
    assert all(len(record) == 4 for record in records)
    for _, iterations, salt, expected_hash in records:
        actual_hash = hashlib.pbkdf2_hmac(
            "sha1",
            b"lakehouse-development-only",
            bytes.fromhex(salt),
            int(iterations),
            dklen=len(expected_hash) // 2,
        ).hex()
        assert int(iterations) >= 10_000
        assert actual_hash == expected_hash


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
