from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_orphan_plan.py PLAN.json")
    plan = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    action = next(
        action
        for action in plan["actions"]
        if action["action_type"] == "inspect_orphan_files"
    )

    assert plan["schema_version"] == "1.0"
    assert action["reason_code"] == "scheduled_inventory"
    assert action["parameters"]["older_than"].endswith("+00:00")
    assert action["safety_bounds"]["dry_run_required"] is True
    assert action["safety_bounds"]["max_concurrent_jobs"] == 1
    assert action["safety_bounds"]["max_orphan_files"] == 1000
    print(json.dumps({"status": "ready", "action_id": action["action_id"]}))


if __name__ == "__main__":
    main()
