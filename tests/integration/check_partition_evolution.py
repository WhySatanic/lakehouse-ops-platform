from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    initial_id = report["before_evolution"]["snapshot_id"]
    changed_id = report["after_spec_change"]["snapshot_id"]
    evolved_id = report["after_partitioned_append"]["snapshot_id"]
    files = report["after_partitioned_append"]["files"]
    initial_rows = [
        [1, "2026-08-01 10:00:00", "before-a"],
        [2, "2026-08-01 11:00:00", "before-b"],
    ]
    current_rows = [
        *initial_rows,
        [3, "2026-08-02 09:00:00", "after-a"],
        [4, "2026-08-03 09:00:00", "after-b"],
    ]
    checks = {
        "schema_version": report["schema_version"] == "1.0",
        "status": report["status"] == "succeeded",
        "table": report["table"] == "lakehouse.ops.partition_evolution_fixture",
        "snapshot_ids": isinstance(initial_id, int)
        and isinstance(evolved_id, int)
        and initial_id > 0
        and evolved_id > 0
        and changed_id == initial_id
        and evolved_id != initial_id,
        "before_evolution": report["before_evolution"]
        == {"snapshot_id": initial_id, "rows": initial_rows},
        "after_spec_change": report["after_spec_change"]
        == {
            "snapshot_id": initial_id,
            "partition_field": "day(event_ts) AS event_day",
        },
        "after_partitioned_append": report["after_partitioned_append"]["rows"]
        == current_rows,
        "file_layout": [
            (item["spec_id"], item["event_day"], item["record_count"])
            for item in files
        ]
        == [
            (0, None, 2),
            (1, "2026-08-02", 1),
            (1, "2026-08-03", 1),
        ],
        "file_counts": all(
            isinstance(item["file_count"], int) and item["file_count"] > 0
            for item in files
        ),
        "compatibility": report["compatibility"]
        == {
            "partition_change_created_data_snapshot": False,
            "old_file_partition_is_null": True,
            "mixed_spec_ids_readable": True,
            "historical_snapshot_preserved": True,
        },
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise SystemExit(f"partition evolution evidence failed: {', '.join(failed)}")
    print(
        json.dumps(
            {
                "status": "ready",
                "initial_snapshot_id": initial_id,
                "evolved_snapshot_id": evolved_id,
                "spec_ids": [0, 1],
                "current_rows": len(current_rows),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
