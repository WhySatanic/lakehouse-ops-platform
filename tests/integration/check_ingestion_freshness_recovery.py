from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def validate_report(report: object) -> None:
    if not isinstance(report, dict):
        raise ValueError("recovery report must be an object")
    if (
        report.get("schema_version") != "1.0"
        or report.get("status") != "succeeded"
        or report.get("table") != "lakehouse.silver.weather_hourly"
    ):
        raise ValueError("recovery report header is invalid")
    recovered_at = _timestamp(report.get("recovered_at"), "recovered_at")
    states: dict[str, dict[str, Any]] = {}
    for name in ("before", "after"):
        state = report.get(name)
        if not isinstance(state, dict):
            raise ValueError(f"{name} state must be an object")
        if state.get("row_count") != 2:
            raise ValueError(f"{name} row count is not preserved")
        snapshot_id = state.get("snapshot_id")
        if not isinstance(snapshot_id, str) or not snapshot_id.isdigit():
            raise ValueError(f"{name} snapshot ID is invalid")
        if not SHA256.fullmatch(str(state.get("content_sha256", ""))):
            raise ValueError(f"{name} content digest is invalid")
        _timestamp(state.get("latest_ingested_at"), f"{name}.latest_ingested_at")
        states[name] = state
    before_time = _timestamp(
        states["before"]["latest_ingested_at"], "before.latest_ingested_at"
    )
    after_time = _timestamp(
        states["after"]["latest_ingested_at"], "after.latest_ingested_at"
    )
    if (recovered_at - before_time).total_seconds() <= 900:
        raise ValueError("pre-recovery ingestion was not stale")
    if after_time != recovered_at:
        raise ValueError("post-recovery ingestion timestamp does not match recovery")
    if states["before"]["snapshot_id"] == states["after"]["snapshot_id"]:
        raise ValueError("recovery did not advance the Iceberg snapshot")
    if states["before"]["content_sha256"] != states["after"]["content_sha256"]:
        raise ValueError("recovery changed business content")
    expected = {
        "row_count_preserved": True,
        "content_preserved": True,
        "snapshot_advanced": True,
        "freshness_recovered": True,
    }
    if report.get("invariants") != expected:
        raise ValueError("recovery invariants are incomplete")


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: check_ingestion_freshness_recovery.py REPORT", file=sys.stderr)
        return 2
    try:
        report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
        validate_report(report)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"ingestion freshness recovery check failed: {error}", file=sys.stderr)
        return 1
    print("Ingestion freshness recovery evidence is valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
