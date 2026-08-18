from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: check_snapshot_expiration.py DRY_RUN.json EXECUTION.json")
    dry_run = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    execution = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))

    assert dry_run["status"] == "dry_run"
    assert dry_run["action_type"] == "expire_snapshots"
    assert dry_run["applied"] is False
    assert dry_run["before"] == dry_run["after"]

    assert execution["status"] == "succeeded"
    assert execution["action_type"] == "expire_snapshots"
    assert execution["applied"] is True
    assert execution["plan_id"] == dry_run["plan_id"]
    assert execution["action_id"] == dry_run["action_id"]
    assert execution["before"]["snapshot_count"] >= 6
    assert execution["after"]["snapshot_count"] == 2
    assert execution["after"]["snapshot_id"] == execution["before"]["snapshot_id"]
    assert execution["after"]["data_file_count"] == execution["before"]["data_file_count"]
    assert execution["after"]["record_count"] == execution["before"]["record_count"] == 4
    assert execution["after"]["manifest_count"] == execution["before"]["manifest_count"]
    print(
        json.dumps(
            {
                "status": "ready",
                "snapshots_before": execution["before"]["snapshot_count"],
                "snapshots_after": execution["after"]["snapshot_count"],
                "records": execution["after"]["record_count"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
