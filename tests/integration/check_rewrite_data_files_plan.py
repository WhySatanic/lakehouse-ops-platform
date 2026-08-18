from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_rewrite_data_files_plan.py PLAN.json")
    plan = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))

    assert plan["schema_version"] == "1.0"
    assert plan["status"] == "maintenance_recommended"
    assert plan["table"] == "lakehouse.ops.maintenance_fixture"
    actions = [
        action for action in plan["actions"] if action["action_type"] == "rewrite_data_files"
    ]
    assert len(actions) == 1
    safety = actions[0]["safety_bounds"]
    assert safety["dry_run_required"] is True
    assert safety["max_concurrent_jobs"] == 1
    assert safety["max_files_to_rewrite"] == 1000
    assert safety["expected_snapshot_id"] == plan["source"]["current_snapshot_id"]
    print(json.dumps({"status": "ready", "action_id": actions[0]["action_id"]}))


if __name__ == "__main__":
    main()
