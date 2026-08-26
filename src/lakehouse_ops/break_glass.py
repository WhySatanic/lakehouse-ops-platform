from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

MAX_LEASE_TTL = timedelta(hours=1)


class BreakGlassError(ValueError):
    pass


def apply_break_glass_lease(
    model: dict[str, Any],
    lease_path: Path,
    *,
    now: datetime | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    lease = _load_lease(lease_path)
    role = _required_string(lease, "role")
    if role not in model["roles"]:
        raise BreakGlassError(f"break-glass lease references unknown role: {role}")

    user = _required_string(lease, "user")
    approved_by = _required_string(lease, "approved_by")
    if approved_by == user:
        raise BreakGlassError("break-glass approver must differ from the user")

    issued_at = _utc_timestamp(lease, "issued_at")
    expires_at = _utc_timestamp(lease, "expires_at")
    if expires_at <= issued_at:
        raise BreakGlassError("break-glass expires_at must be after issued_at")
    if expires_at - issued_at > MAX_LEASE_TTL:
        raise BreakGlassError("break-glass lease TTL must not exceed 1 hour")

    evaluated_at = now or datetime.now(UTC)
    if evaluated_at.tzinfo is None or evaluated_at.utcoffset() != timedelta(0):
        raise BreakGlassError("break-glass evaluation time must be UTC")

    status = "active"
    if evaluated_at < issued_at:
        status = "not_yet_valid"
    elif evaluated_at >= expires_at:
        status = "expired"

    effective_model = copy.deepcopy(model)
    if status == "active":
        effective_model["bindings"].append({"users": [user], "roles": [role]})

    report = {
        "schema_version": "1.0",
        "grant_id": _required_string(lease, "grant_id"),
        "status": status,
        "user": user,
        "role": role,
        "approved_by": approved_by,
        "ticket": _required_string(lease, "ticket"),
        "reason": _required_string(lease, "reason"),
        "issued_at": issued_at.isoformat().replace("+00:00", "Z"),
        "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
        "evaluated_at": evaluated_at.isoformat().replace("+00:00", "Z"),
    }
    return effective_model, report


def _load_lease(path: Path) -> dict[str, Any]:
    try:
        lease = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise BreakGlassError(f"invalid break-glass JSON: {error}") from error
    if not isinstance(lease, dict):
        raise BreakGlassError("break-glass lease must be a JSON object")
    if lease.get("schema_version") != "1.0":
        raise BreakGlassError("unsupported break-glass schema_version")
    return lease


def _required_string(document: dict[str, Any], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value.strip():
        raise BreakGlassError(f"break-glass {key} must be a non-empty string")
    return value.strip()


def _utc_timestamp(document: dict[str, Any], key: str) -> datetime:
    value = _required_string(document, key)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise BreakGlassError(f"break-glass {key} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise BreakGlassError(f"break-glass {key} must use UTC")
    return parsed
