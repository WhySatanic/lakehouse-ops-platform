from __future__ import annotations

import json
import sys
from pathlib import Path

ROWS_PER_DAY = 2_048
DAYS = 32
TOTAL_ROWS = ROWS_PER_DAY * DAYS


def positive_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_partition_pruning_fixture.py REPORT.json")
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    tables = report.get("tables", {})
    unpartitioned = tables.get("unpartitioned", {})
    partitioned = tables.get("partitioned", {})
    expected_checksum = sum(range(15 * ROWS_PER_DAY, 16 * ROWS_PER_DAY))

    checks = {
        "schema": report.get("schema_version") == "1.0",
        "status": report.get("status") == "ready",
        "experiment": report.get("experiment") == "iceberg_partition_pruning",
        "target_day": report.get("target_day") == "2026-01-16",
        "shape": report.get("rows_per_day") == ROWS_PER_DAY
        and report.get("days") == DAYS
        and report.get("total_rows") == TOTAL_ROWS,
        "unpartitioned_records": unpartitioned.get("record_count") == TOTAL_ROWS,
        "partitioned_records": partitioned.get("record_count") == TOTAL_ROWS,
        "unpartitioned_partitions": unpartitioned.get("partition_count") == 1,
        "partitioned_partitions": partitioned.get("partition_count") == DAYS,
        "unpartitioned_files": positive_integer(unpartitioned.get("file_count")),
        "partitioned_files": partitioned.get("file_count", 0) >= DAYS,
        "snapshots": positive_integer(unpartitioned.get("snapshot_id"))
        and positive_integer(partitioned.get("snapshot_id")),
        "sizes": positive_integer(unpartitioned.get("total_size_bytes"))
        and positive_integer(partitioned.get("total_size_bytes")),
        "filtered_rows": report.get("filtered_result", {}).get("row_count")
        == ROWS_PER_DAY,
        "filtered_checksum": report.get("filtered_result", {}).get(
            "event_id_checksum"
        )
        == expected_checksum,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise SystemExit(f"partition pruning fixture failed: {', '.join(failed)}")
    print(
        json.dumps(
            {
                "status": "ready",
                "rows": TOTAL_ROWS,
                "partitions": partitioned["partition_count"],
                "target_rows": ROWS_PER_DAY,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
