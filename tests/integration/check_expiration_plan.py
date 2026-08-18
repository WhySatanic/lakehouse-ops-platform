from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_expiration_plan.py PLAN.json")
    plan = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    action = next(
        action for action in plan["actions"] if action["action_type"] == "expire_snapshots"
    )
    history = action["safety_bounds"]["expected_history_snapshot_ids"]
    targets = action["parameters"]["snapshot_ids"]

    assert plan["schema_version"] == "1.0"
    assert len(history) >= 6
    assert len(targets) == len(history) - 2
    assert plan["source"]["current_snapshot_id"] not in targets
    assert set(targets).issubset(history)
    assert action["safety_bounds"]["dry_run_required"] is True
    assert action["safety_bounds"]["max_concurrent_jobs"] == 1
    assert action["safety_bounds"]["max_snapshots_to_expire"] == 50
    print(json.dumps({"status": "ready", "snapshots_to_expire": len(targets)}))


if __name__ == "__main__":
    main()
