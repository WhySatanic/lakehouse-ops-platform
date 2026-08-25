from __future__ import annotations

import json
import sys
from pathlib import Path

EXPECTED_ROWS = [[1, "committed-a"], [2, "committed-b"], [3, "committed-c"]]


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_interrupted_write_reconciliation.py REPORT")
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    before = report["before"]
    interrupted = report["interrupted"]
    reconciled = report["reconciled"]
    candidate = interrupted["candidate"]
    checks = {
        "schema_version": report["schema_version"] == "1.0",
        "status": report["status"] == "recovered",
        "scenario": report["scenario"] == "interrupted_write_before_metadata_commit",
        "table": report["table"] == "lakehouse.ops.interrupted_write_fixture",
        "injection_point": report["injection_point"]
        == "after_data_upload_before_metadata_commit",
        "injected_error": report["injected_error"]
        == "injected interruption after data upload and before metadata commit",
        "baseline": before["rows"] == EXPECTED_ROWS
        and report["source_file"] in before["referenced_files"],
        "interrupted_state": {
            key: interrupted[key] for key in ("snapshot_id", "rows", "referenced_files")
        }
        == before,
        "candidate": candidate["location"].endswith(".parquet")
        and isinstance(candidate["etag"], str)
        and bool(candidate["etag"])
        and isinstance(candidate["size_bytes"], int)
        and candidate["size_bytes"] > 0
        and candidate["exists"] is True
        and candidate["referenced"] is False,
        "reconciled_state": {
            key: reconciled[key] for key in ("snapshot_id", "rows", "referenced_files")
        }
        == before,
        "candidate_removed": reconciled["candidate_exists"] is False,
        "post_conditions": report["post_conditions"]
        == {
            "snapshot_unchanged": True,
            "rows_unchanged": True,
            "referenced_files_unchanged": True,
            "exact_candidate_removed": True,
        },
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise SystemExit(
            "interrupted-write reconciliation evidence failed: " + ", ".join(failed)
        )
    print(
        json.dumps(
            {
                "status": "ready",
                "snapshot_id": before["snapshot_id"],
                "rows": len(before["rows"]),
                "removed_candidate": candidate["location"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
