from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    initial_id = report["initial"]["snapshot_id"]
    evolved_id = report["after_add"]["snapshot_id"]
    checks = {
        "schema_version": report["schema_version"] == "1.0",
        "status": report["status"] == "succeeded",
        "table": report["table"] == "lakehouse.ops.schema_evolution_fixture",
        "snapshot_ids": isinstance(initial_id, int)
        and isinstance(evolved_id, int)
        and initial_id > 0
        and evolved_id > 0
        and initial_id != evolved_id,
        "initial": report["initial"]
        == {
            "snapshot_id": initial_id,
            "columns": ["event_id", "payload"],
            "rows": [[1, "stable"]],
        },
        "after_add": report["after_add"]
        == {
            "snapshot_id": evolved_id,
            "columns": ["event_id", "payload", "severity"],
            "rows": [[1, "stable", None], [2, "regression", "warning"]],
        },
        "after_rename": report["after_rename"]
        == {
            "snapshot_id": evolved_id,
            "columns": ["event_id", "message", "severity"],
            "rows": [[1, "stable", None], [2, "regression", "warning"]],
        },
        "compatibility": report["compatibility"]
        == {
            "added_column_defaults_to_null": True,
            "historical_snapshot_schemas_preserved": True,
            "renamed_values_preserved": True,
            "rename_created_data_snapshot": False,
        },
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise SystemExit(f"schema evolution evidence failed: {', '.join(failed)}")
    print(
        json.dumps(
            {
                "status": "ready",
                "initial_snapshot_id": initial_id,
                "evolved_snapshot_id": evolved_id,
                "current_columns": report["after_rename"]["columns"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
