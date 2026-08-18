from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


def load_report(path: Path) -> dict[str, Any]:
    report = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise AssertionError("metadata report must be a JSON object")
    return report


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_iceberg_metadata_report.py REPORT.json")
    report = load_report(Path(sys.argv[1]))

    assert report["schema_version"] == "1.0"
    assert report["status"] == "ready"
    assert report["table"] == "lakehouse.silver.weather_hourly"
    datetime.fromisoformat(report["collected_at"])

    snapshots = report["snapshots"]
    assert snapshots["count"] > 0
    assert snapshots["current_id"].isdigit()
    assert snapshots["operation"] in {"append", "delete", "overwrite", "replace"}
    assert len(snapshots["history"]) == snapshots["count"]
    assert snapshots["history"][-1]["snapshot_id"] == snapshots["current_id"]
    assert report["references"] == [
        {
            "name": "main",
            "reference_type": "BRANCH",
            "snapshot_id": snapshots["current_id"],
        }
    ]

    files = report["files"]
    assert files["count"] > 0
    assert files["records"] == 2
    assert files["total_size_bytes"] > 0
    assert 0 < files["min_size_bytes"] <= files["max_size_bytes"]
    assert files["delete_file_count"] == 0

    manifests = report["manifests"]
    assert manifests["count"] > 0
    assert manifests["total_size_bytes"] > 0
    assert manifests["added_files"] + manifests["existing_files"] > 0

    partitions = report["partitions"]
    assert partitions["count"] > 0
    assert partitions["records"] == files["records"]
    assert partitions["files"] == files["count"]
    assert partitions["total_size_bytes"] == files["total_size_bytes"]

    print(
        json.dumps(
            {
                "status": "ready",
                "table": report["table"],
                "snapshots": snapshots["count"],
                "files": files["count"],
                "partitions": partitions["count"],
                "records": files["records"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
