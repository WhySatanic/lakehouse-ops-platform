from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

EXPECTED_CASES = {
    "platform_admin_reads_bronze": "allowed",
    "data_engineer_reads_bronze": "allowed",
    "analytics_engineer_reads_silver": "allowed",
    "analytics_engineer_checksum_is_masked": "allowed",
    "platform_admin_checksum_is_visible": "allowed",
    "operator_reads_system": "allowed",
    "analytics_engineer_cannot_read_bronze": "denied",
    "analyst_cannot_read_silver": "denied",
    "service_ingest_cannot_read_silver": "denied",
    "unknown_user_cannot_read_lakehouse": "denied",
    "unknown_user_cannot_read_system": "denied",
    "data_engineer_cannot_create_schema": "denied",
}


def validate(report: dict[str, Any], *, expected_mode: str = "file") -> list[str]:
    errors: list[str] = []
    if report.get("schema_version") != "1.0":
        errors.append("schema_version")
    if report.get("status") != "succeeded":
        errors.append("status")

    policy = report.get("policy")
    if not isinstance(policy, dict):
        errors.append("policy")
    else:
        if policy.get("engine") != "trino":
            errors.append("policy.engine")
        if policy.get("mode") != expected_mode:
            errors.append("policy.mode")
        if policy.get("default") != "deny":
            errors.append("policy.default")
        if policy.get("authentication_enforced") is not False:
            errors.append("policy.authentication_enforced")

    cases = report.get("cases")
    if not isinstance(cases, list):
        return [*errors, "cases"]
    actual = {
        case.get("id"): case.get("result")
        for case in cases
        if isinstance(case, dict)
    }
    if actual != EXPECTED_CASES:
        errors.append("cases.coverage")
    for case in cases:
        if not isinstance(case, dict):
            errors.append("cases.shape")
            continue
        expectation = case.get("expectation")
        result = case.get("result")
        if (expectation, result) not in {("allow", "allowed"), ("deny", "denied")}:
            errors.append(f"cases.{case.get('id')}.outcome")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--mode", choices=("file", "ranger"), default="file")
    args = parser.parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    errors = validate(report, expected_mode=args.mode)
    if errors:
        raise SystemExit(f"Trino authorization evidence failed: {', '.join(errors)}")
    print(
        json.dumps(
            {
                "status": "ready",
                "policy": "deny-by-default",
                "mode": args.mode,
                "allowed_cases": 6,
                "denied_cases": 6,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
