from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: check_maintenance_execution.py DRY_RUN.json EXECUTION.json")
    dry_run = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    execution = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))

    assert dry_run["schema_version"] == "1.0"
    assert dry_run["status"] == "dry_run"
    assert dry_run["applied"] is False
    assert dry_run["before"] == dry_run["after"]

    assert execution["schema_version"] == "1.0"
    assert execution["status"] == "succeeded"
    assert execution["applied"] is True
    assert execution["plan_id"] == dry_run["plan_id"]
    assert execution["action_id"] == dry_run["action_id"]
    assert execution["before"]["snapshot_id"] == dry_run["before"]["snapshot_id"]
    assert execution["after"]["snapshot_id"] != execution["before"]["snapshot_id"]
    assert execution["before"]["record_count"] == 4
    assert execution["after"]["record_count"] == 4
    assert execution["after"]["data_file_count"] < execution["before"]["data_file_count"]
    assert execution["procedure_result"]["rewritten_data_files_count"] >= 2
    assert execution["procedure_result"]["failed_data_files_count"] == 0

    print(
        json.dumps(
            {
                "status": "ready",
                "files_before": execution["before"]["data_file_count"],
                "files_after": execution["after"]["data_file_count"],
                "records": execution["after"]["record_count"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
