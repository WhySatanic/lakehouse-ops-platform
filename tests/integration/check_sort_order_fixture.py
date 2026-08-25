from __future__ import annotations

import json
import sys
from pathlib import Path

TOTAL_ROWS = 65_536
RANGE_START = 30_000
RANGE_SIZE = 128


def positive_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_sort_order_fixture.py REPORT.json")
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    tables = report.get("tables", {})
    baseline = tables.get("baseline", {})
    sorted_table = tables.get("sorted", {})
    expected_checksum = sum(range(RANGE_START, RANGE_START + RANGE_SIZE))

    checks = {
        "schema": report.get("schema_version") == "1.0",
        "status": report.get("status") == "ready",
        "experiment": report.get("experiment") == "iceberg_sort_order",
        "shape": report.get("total_rows") == TOTAL_ROWS
        and report.get("target_files") == 16,
        "range": report.get("range")
        == {
            "start": RANGE_START,
            "end_exclusive": RANGE_START + RANGE_SIZE,
            "size": RANGE_SIZE,
        },
        "records": baseline.get("record_count") == TOTAL_ROWS
        and sorted_table.get("record_count") == TOTAL_ROWS,
        "unpartitioned": baseline.get("partition_count") == 1
        and sorted_table.get("partition_count") == 1,
        "files": baseline.get("file_count", 0) >= 8
        and sorted_table.get("file_count", 0) >= 8,
        "snapshots": positive_integer(baseline.get("snapshot_id"))
        and positive_integer(sorted_table.get("snapshot_id")),
        "sizes": positive_integer(baseline.get("total_size_bytes"))
        and positive_integer(sorted_table.get("total_size_bytes")),
        "filtered_rows": report.get("filtered_result", {}).get("row_count")
        == RANGE_SIZE,
        "filtered_checksum": report.get("filtered_result", {}).get(
            "event_id_checksum"
        )
        == expected_checksum,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise SystemExit(f"sort order fixture failed: {', '.join(failed)}")
    print(
        json.dumps(
            {
                "status": "ready",
                "rows": TOTAL_ROWS,
                "baseline_files": baseline["file_count"],
                "sorted_files": sorted_table["file_count"],
                "target_rows": RANGE_SIZE,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
