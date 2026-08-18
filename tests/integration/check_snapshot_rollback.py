from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    target = report["time_travel"]["snapshot_id"]
    previous = report["before"]["snapshot_id"]
    checks = {
        "schema_version": report["schema_version"] == "1.0",
        "status": report["status"] == "succeeded",
        "table": report["table"] == "lakehouse.ops.snapshot_recovery_fixture",
        "different_snapshots": isinstance(target, int)
        and isinstance(previous, int)
        and target > 0
        and previous > 0
        and target != previous,
        "before_rows": report["before"]["rows"]
        == [[1, "stable"], [2, "regression"]],
        "historical_rows": report["time_travel"]["rows"] == [[1, "stable"]],
        "rollback_result": report["rollback"]
        == {
            "previous_snapshot_id": previous,
            "current_snapshot_id": target,
        },
        "restored_rows": report["after"]
        == {"snapshot_id": target, "rows": [[1, "stable"]]},
        "abandoned_readable": report["abandoned_snapshot"]
        == {
            "snapshot_id": previous,
            "readable": True,
            "rows": [[1, "stable"], [2, "regression"]],
        },
        "lineage": report["history"]
        == {
            "current_ancestor_ids": [target],
            "abandoned_snapshot_ids": [previous],
        },
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise SystemExit(f"snapshot rollback evidence failed: {', '.join(failed)}")
    print(
        json.dumps(
            {
                "status": "ready",
                "target_snapshot_id": target,
                "abandoned_snapshot_id": previous,
                "restored_rows": 1,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
