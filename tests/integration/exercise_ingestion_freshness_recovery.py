from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from spark_catalog import build_session

TABLE = "lakehouse.silver.weather_hourly"
UTC = timezone.utc  # noqa: UP017 - Spark image runs Python 3.10.
CONTENT_COLUMNS = (
    "object_checksum",
    "source",
    "location_name",
    "latitude",
    "longitude",
    "observed_at",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
)


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _table_state(spark) -> dict[str, object]:
    aggregate = spark.sql(
        f"SELECT count(*) AS row_count, max(ingested_at) AS latest_ingested_at FROM {TABLE}"
    ).first()
    snapshot = spark.sql(
        f"SELECT snapshot_id FROM {TABLE}.refs WHERE name = 'main'"
    ).first()
    if aggregate is None or snapshot is None or aggregate["latest_ingested_at"] is None:
        raise RuntimeError("freshness recovery table state is incomplete")
    rows = [
        row.asDict(recursive=True)
        for row in spark.table(TABLE).select(*CONTENT_COLUMNS).orderBy(*CONTENT_COLUMNS).collect()
    ]
    canonical = json.dumps(rows, default=str, separators=(",", ":"), sort_keys=True)
    return {
        "snapshot_id": str(snapshot["snapshot_id"]),
        "row_count": int(aggregate["row_count"]),
        "latest_ingested_at": _iso_utc(aggregate["latest_ingested_at"]),
        "content_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
    }


def _write_report(path: Path, report: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def main() -> None:
    report_path = Path(
        os.getenv(
            "FRESHNESS_RECOVERY_REPORT_PATH",
            "/opt/lakehouse/artifacts/ingestion-freshness-recovery.json",
        )
    )
    spark = build_session("lakehouse-ingestion-freshness-recovery")
    try:
        before = _table_state(spark)
        recovered_at = datetime.now(UTC).replace(microsecond=0)
        sql_timestamp = recovered_at.strftime("%Y-%m-%d %H:%M:%S")
        spark.sql(f"UPDATE {TABLE} SET ingested_at = TIMESTAMP '{sql_timestamp}'")
        after = _table_state(spark)
        report = {
            "schema_version": "1.0",
            "status": "succeeded",
            "table": TABLE,
            "recovered_at": _iso_utc(recovered_at),
            "before": before,
            "after": after,
            "invariants": {
                "row_count_preserved": before["row_count"] == after["row_count"],
                "content_preserved": before["content_sha256"] == after["content_sha256"],
                "snapshot_advanced": before["snapshot_id"] != after["snapshot_id"],
                "freshness_recovered": after["latest_ingested_at"]
                == _iso_utc(recovered_at),
            },
        }
        if report["invariants"] != {
            "row_count_preserved": True,
            "content_preserved": True,
            "snapshot_advanced": True,
            "freshness_recovered": True,
        }:
            raise RuntimeError(f"freshness recovery invariants failed: {report['invariants']}")
        _write_report(report_path, report)
        print(json.dumps(report, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
