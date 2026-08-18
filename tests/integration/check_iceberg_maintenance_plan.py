from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def load_plan(path: Path) -> dict[str, Any]:
    plan = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(plan, dict):
        raise AssertionError("maintenance plan must be a JSON object")
    return plan


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_iceberg_maintenance_plan.py PLAN.json")
    plan = load_plan(Path(sys.argv[1]))

    assert plan["schema_version"] == "1.0"
    assert plan["plan_id"].startswith("plan-")
    assert plan["status"] == "healthy"
    assert plan["table"] == "lakehouse.silver.weather_hourly"
    assert plan["source"]["metadata_schema_version"] == "1.0"
    assert plan["source"]["current_snapshot_id"].isdigit()
    assert plan["policy"]["target_file_size_bytes"] == 128 * 1024 * 1024
    assert [check["rule"] for check in plan["checks"]] == [
        "small_data_files",
        "manifest_density",
        "snapshot_retention",
    ]
    assert plan["actions"] == []

    print(json.dumps({"status": "ready", "plan_id": plan["plan_id"]}, sort_keys=True))


if __name__ == "__main__":
    main()
