from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_orphan_inspection.py REPORT.json")
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))

    assert report["schema_version"] == "1.0"
    assert report["status"] == "inspection_complete"
    assert report["action_type"] == "inspect_orphan_files"
    assert report["applied"] is False
    assert report["before"] == report["after"]
    assert report["procedure_result"]["orphan_file_count"] == len(
        report["candidate_files"]
    )
    assert report["procedure_result"]["orphan_file_count"] == 0
    assert report["candidate_set_id"].startswith("orphans-")
    print(
        json.dumps(
            {
                "status": "ready",
                "orphan_file_count": report["procedure_result"][
                    "orphan_file_count"
                ],
                "candidate_set_id": report["candidate_set_id"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
