from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from lakehouse_ops.access_policy import load_access_policy
from lakehouse_ops.break_glass import BreakGlassError, apply_break_glass_lease

ROOT = Path(__file__).parents[1]
MODEL_PATH = ROOT / "config" / "access" / "role-policy.json"
SCHEMA_PATH = ROOT / "config" / "access" / "break-glass-lease.schema.json"
NOW = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)


def test_break_glass_schema_accepts_supported_lease() -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)

    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    validator.validate(_lease_document(issued_at=NOW - timedelta(minutes=5)))


def test_break_glass_schema_rejects_unreviewed_extension() -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    lease = _lease_document(issued_at=NOW - timedelta(minutes=5))
    lease["bypass_validation"] = True

    errors = list(Draft202012Validator(schema).iter_errors(lease))

    assert len(errors) == 1
    assert errors[0].validator == "additionalProperties"


def test_active_lease_adds_approved_role_binding(tmp_path: Path) -> None:
    lease_path = _write_lease(tmp_path, issued_at=NOW - timedelta(minutes=5))

    effective, report = apply_break_glass_lease(
        load_access_policy(MODEL_PATH), lease_path, now=NOW
    )

    assert effective["bindings"][-1] == {
        "users": ["incident-responder"],
        "roles": ["platform_admin"],
    }
    assert report == {
        "schema_version": "1.0",
        "grant_id": "BG-2026-0042",
        "status": "active",
        "user": "incident-responder",
        "role": "platform_admin",
        "approved_by": "incident-commander",
        "ticket": "INC-4242",
        "reason": "restore lakehouse query service",
        "issued_at": "2026-08-26T11:55:00Z",
        "expires_at": "2026-08-26T12:25:00Z",
        "evaluated_at": "2026-08-26T12:00:00Z",
    }


@pytest.mark.parametrize(
    ("issued_at", "expires_at", "expected_status"),
    [
        (NOW + timedelta(minutes=5), NOW + timedelta(minutes=30), "not_yet_valid"),
        (NOW - timedelta(minutes=30), NOW, "expired"),
    ],
)
def test_inactive_lease_does_not_change_bindings(
    tmp_path: Path,
    issued_at: datetime,
    expires_at: datetime,
    expected_status: str,
) -> None:
    model = load_access_policy(MODEL_PATH)
    lease_path = _write_lease(tmp_path, issued_at=issued_at, expires_at=expires_at)

    effective, report = apply_break_glass_lease(model, lease_path, now=NOW)

    assert effective == model
    assert effective is not model
    assert report["status"] == expected_status


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"schema_version": "2.0"}, "unsupported break-glass schema_version"),
        ({"user": ""}, "break-glass user must be a non-empty string"),
        ({"approved_by": "incident-responder"}, "approver must differ"),
        ({"role": "missing"}, "references unknown role"),
        ({"issued_at": "not-a-date"}, "issued_at must be an ISO-8601 timestamp"),
        ({"issued_at": "2026-08-26T11:55:00"}, "issued_at must use UTC"),
        (
            {"expires_at": "2026-08-26T11:00:00Z"},
            "expires_at must be after issued_at",
        ),
        (
            {"expires_at": "2026-08-26T13:30:00Z"},
            "TTL must not exceed 1 hour",
        ),
        ({"ticket": None}, "break-glass ticket must be a non-empty string"),
    ],
)
def test_rejects_invalid_lease(
    tmp_path: Path, change: dict[str, object], message: str
) -> None:
    lease_path = _write_lease(tmp_path, issued_at=NOW - timedelta(minutes=5), change=change)

    with pytest.raises(BreakGlassError, match=message):
        apply_break_glass_lease(load_access_policy(MODEL_PATH), lease_path, now=NOW)


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ([], "break-glass lease must be a JSON object"),
        ("{", "invalid break-glass JSON"),
    ],
)
def test_rejects_invalid_document(tmp_path: Path, document: object, message: str) -> None:
    path = tmp_path / "lease.json"
    content = document if isinstance(document, str) else json.dumps(document)
    path.write_text(content, encoding="utf-8")

    with pytest.raises(BreakGlassError, match=message):
        apply_break_glass_lease(load_access_policy(MODEL_PATH), path, now=NOW)


def test_rejects_non_utc_evaluation_time(tmp_path: Path) -> None:
    lease_path = _write_lease(tmp_path, issued_at=NOW - timedelta(minutes=5))

    with pytest.raises(BreakGlassError, match="evaluation time must be UTC"):
        apply_break_glass_lease(
            load_access_policy(MODEL_PATH), lease_path, now=NOW.replace(tzinfo=None)
        )


def _write_lease(
    tmp_path: Path,
    *,
    issued_at: datetime,
    expires_at: datetime | None = None,
    change: dict[str, object] | None = None,
) -> Path:
    document = _lease_document(issued_at=issued_at, expires_at=expires_at)
    document.update(change or {})
    path = tmp_path / "lease.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _lease_document(
    *, issued_at: datetime, expires_at: datetime | None = None
) -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "grant_id": "BG-2026-0042",
        "user": "incident-responder",
        "role": "platform_admin",
        "approved_by": "incident-commander",
        "ticket": "INC-4242",
        "reason": "restore lakehouse query service",
        "issued_at": issued_at.isoformat().replace("+00:00", "Z"),
        "expires_at": (expires_at or issued_at + timedelta(minutes=30))
        .isoformat()
        .replace("+00:00", "Z"),
    }
