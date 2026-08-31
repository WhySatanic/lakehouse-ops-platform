from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

EXPECTED_REPORT = {
    "schema_version": "1.0",
    "status": "ready",
    "engine": "clickhouse",
    "mode": "direct_iceberg_s3",
    "clickhouse_version": "26.3.25.2",
    "table_url": "http://minio:9000/lakehouse/warehouse/silver/weather_hourly",
    "silver_rows": 2,
    "reject_rows": 1,
    "duplicate_keys": 0,
    "latest_survivor": 1,
}


def validate(report: Any) -> None:
    if not isinstance(report, dict):
        raise ValueError("ClickHouse serving evidence must be an object")
    if report != EXPECTED_REPORT:
        differing = sorted(
            key
            for key in set(report) | set(EXPECTED_REPORT)
            if report.get(key) != EXPECTED_REPORT.get(key)
        )
        raise ValueError(
            "ClickHouse serving evidence differs from the acceptance contract: "
            + ", ".join(differing)
        )


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_clickhouse_serving.py REPORT")
    path = Path(sys.argv[1])
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
        validate(report)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        raise SystemExit(f"ClickHouse serving evidence failed: {error}") from error


if __name__ == "__main__":
    main()
