from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("grant", type=Path)
    parser.add_argument("allowed", type=Path)
    parser.add_argument("revoke", type=Path)
    parser.add_argument("denied", type=Path)
    args = parser.parse_args()

    grant = _read(args.grant)
    allowed = _read(args.allowed)
    revoke = _read(args.revoke)
    denied = _read(args.denied)
    errors: list[str] = []

    _check_sync(grant, "active", errors)
    _check_sync(revoke, "expired", errors)
    _check_access(allowed, "allowed", errors)
    _check_access(denied, "denied", errors)
    if grant.get("break_glass", {}).get("grant_id") != revoke.get("break_glass", {}).get(
        "grant_id"
    ):
        errors.append("grant_id")
    if errors:
        raise SystemExit(f"break-glass drill evidence failed: {', '.join(errors)}")

    print(
        json.dumps(
            {
                "schema_version": "1.0",
                "status": "ready",
                "grant_id": grant["break_glass"]["grant_id"],
                "user": grant["break_glass"]["user"],
                "grant": "allowed",
                "expiry": "denied",
            },
            sort_keys=True,
        )
    )


def _check_sync(report: dict[str, Any], status: str, errors: list[str]) -> None:
    prefix = f"sync.{status}"
    if report.get("status") != "synchronized":
        errors.append(f"{prefix}.status")
    lease = report.get("break_glass")
    if not isinstance(lease, dict) or lease.get("status") != status:
        errors.append(f"{prefix}.lease")
    policies = report.get("policies")
    if not isinstance(policies, dict) or policies.get("updated", 0) < 1:
        errors.append(f"{prefix}.policies")


def _check_access(report: dict[str, Any], result: str, errors: list[str]) -> None:
    if report.get("status") != "ready" or report.get("result") != result:
        errors.append(f"access.{result}")


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"evidence must be an object: {path}")
    return value


if __name__ == "__main__":
    main()
