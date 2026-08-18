from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_rewrite_manifests_plan.py PLAN.json")
    plan = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    actions = [
        action for action in plan["actions"] if action["action_type"] == "rewrite_manifests"
    ]

    assert plan["schema_version"] == "1.0"
    assert plan["status"] == "maintenance_recommended"
    assert len(actions) == 1
    assert actions[0]["safety_bounds"]["dry_run_required"] is True
    assert actions[0]["safety_bounds"]["max_concurrent_jobs"] == 1
    assert actions[0]["safety_bounds"]["max_manifests_to_rewrite"] == 1000
    assert "max_files_to_rewrite" not in actions[0]["safety_bounds"]
    print(json.dumps({"status": "ready", "action_id": actions[0]["action_id"]}))


if __name__ == "__main__":
    main()
