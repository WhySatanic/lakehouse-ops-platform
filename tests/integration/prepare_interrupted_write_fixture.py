from __future__ import annotations

import json
from pathlib import Path

from spark_catalog import build_session, required_environment

TABLE = "lakehouse.ops.interrupted_write_fixture"


def main() -> None:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    report_path = Path(required_environment("INTERRUPTED_WRITE_REPORT_PATH"))
    spark = build_session("lakehouse-ops-interrupted-write-fixture")
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.ops "
            f"LOCATION 's3a://{bucket}/warehouse/ops'"
        )
        spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
        spark.sql(
            f"""
            CREATE TABLE {TABLE} (
                event_id long,
                payload string
            )
            USING iceberg
            LOCATION 's3a://{bucket}/warehouse/ops/interrupted_write_fixture'
            TBLPROPERTIES ('format-version' = '2')
            """
        )
        spark.sql(
            f"INSERT INTO {TABLE} VALUES "
            "(1, 'committed-a'), (2, 'committed-b'), (3, 'committed-c')"
        )
        snapshot = spark.sql(
            f"SELECT snapshot_id FROM {TABLE}.refs WHERE name = 'main'"
        ).first()
        if snapshot is None:
            raise RuntimeError("fixture table has no main snapshot")
        rows = [
            list(row)
            for row in spark.sql(
                f"SELECT event_id, payload FROM {TABLE} ORDER BY event_id"
            ).collect()
        ]
        files = [
            str(row["file_path"])
            for row in spark.sql(
                f"SELECT file_path FROM {TABLE}.files "
                "WHERE content = 0 ORDER BY file_path"
            ).collect()
        ]
        if not files:
            raise RuntimeError("fixture table has no committed data file")
        report = {
            "schema_version": "1.0",
            "status": "baseline_ready",
            "scenario": "interrupted_write_before_metadata_commit",
            "table": TABLE,
            "before": {
                "snapshot_id": str(snapshot["snapshot_id"]),
                "rows": rows,
                "referenced_files": files,
            },
            "source_file": files[0],
        }
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(json.dumps(report, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
