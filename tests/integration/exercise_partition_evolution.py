from __future__ import annotations

import json
from pathlib import Path

from spark_catalog import build_session, required_environment

TABLE = "lakehouse.ops.partition_evolution_fixture"


def current_snapshot_id(spark) -> int:
    row = spark.sql(f"SELECT snapshot_id FROM {TABLE}.refs WHERE name = 'main'").first()
    if row is None:
        raise RuntimeError("table has no main snapshot reference")
    return int(row["snapshot_id"])


def table_rows(spark, *, snapshot_id: int | None = None) -> list[list[object]]:
    source = TABLE if snapshot_id is None else f"{TABLE} VERSION AS OF {snapshot_id}"
    frame = spark.sql(
        "SELECT event_id, date_format(event_ts, 'yyyy-MM-dd HH:mm:ss') AS event_ts, "
        f"payload FROM {source} ORDER BY event_id"
    )
    return [list(row) for row in frame.collect()]


def file_layout(spark) -> list[dict[str, object]]:
    rows = spark.sql(
        f"SELECT spec_id, CAST(partition.event_day AS STRING) AS event_day, "
        f"SUM(record_count) AS record_count, COUNT(*) AS file_count FROM {TABLE}.files "
        "GROUP BY spec_id, partition.event_day ORDER BY spec_id, event_day"
    ).collect()
    return [
        {
            "spec_id": int(row["spec_id"]),
            "event_day": row["event_day"],
            "record_count": int(row["record_count"]),
            "file_count": int(row["file_count"]),
        }
        for row in rows
    ]


def write_report(path: Path, report: dict[str, object]) -> None:
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    report_path = Path(required_environment("PARTITION_EVOLUTION_REPORT_PATH"))
    spark = build_session("lakehouse-ops-partition-evolution-drill")
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
                event_ts timestamp,
                payload string
            )
            USING iceberg
            LOCATION 's3a://{bucket}/warehouse/ops/partition_evolution_fixture'
            TBLPROPERTIES ('format-version' = '2')
            """
        )
        spark.sql(
            f"INSERT INTO {TABLE} VALUES "
            "(1, TIMESTAMP '2026-08-01 10:00:00', 'before-a'), "
            "(2, TIMESTAMP '2026-08-01 11:00:00', 'before-b')"
        )
        initial_snapshot_id = current_snapshot_id(spark)
        initial_rows = table_rows(spark, snapshot_id=initial_snapshot_id)

        spark.sql(
            f"ALTER TABLE {TABLE} ADD PARTITION FIELD day(event_ts) AS event_day"
        )
        snapshot_id_after_spec_change = current_snapshot_id(spark)
        if snapshot_id_after_spec_change != initial_snapshot_id:
            raise RuntimeError(
                "partition evolution unexpectedly created a data snapshot: "
                f"before={initial_snapshot_id}, after={snapshot_id_after_spec_change}"
            )

        spark.sql(
            f"INSERT INTO {TABLE} VALUES "
            "(3, TIMESTAMP '2026-08-02 09:00:00', 'after-a'), "
            "(4, TIMESTAMP '2026-08-03 09:00:00', 'after-b')"
        )
        evolved_snapshot_id = current_snapshot_id(spark)
        if evolved_snapshot_id == initial_snapshot_id:
            raise RuntimeError("partitioned append did not create a new snapshot")

        expected_initial_rows = [
            [1, "2026-08-01 10:00:00", "before-a"],
            [2, "2026-08-01 11:00:00", "before-b"],
        ]
        expected_current_rows = [
            *expected_initial_rows,
            [3, "2026-08-02 09:00:00", "after-a"],
            [4, "2026-08-03 09:00:00", "after-b"],
        ]
        expected_layout = [
            (0, None, 2),
            (1, "2026-08-02", 1),
            (1, "2026-08-03", 1),
        ]
        historical_rows = table_rows(spark, snapshot_id=initial_snapshot_id)
        current_rows = table_rows(spark)
        layout = file_layout(spark)
        if initial_rows != expected_initial_rows or historical_rows != expected_initial_rows:
            raise RuntimeError(
                "initial snapshot changed after partition evolution: "
                f"before={initial_rows}, after={historical_rows}"
            )
        if current_rows != expected_current_rows:
            raise RuntimeError(f"unexpected mixed-layout rows: {current_rows}")
        layout_summary = [
            (item["spec_id"], item["event_day"], item["record_count"])
            for item in layout
        ]
        if layout_summary != expected_layout or any(
            item["file_count"] < 1 for item in layout
        ):
            raise RuntimeError(f"unexpected partition file layout: {layout}")

        report = {
            "schema_version": "1.0",
            "status": "succeeded",
            "table": TABLE,
            "before_evolution": {
                "snapshot_id": initial_snapshot_id,
                "rows": historical_rows,
            },
            "after_spec_change": {
                "snapshot_id": snapshot_id_after_spec_change,
                "partition_field": "day(event_ts) AS event_day",
            },
            "after_partitioned_append": {
                "snapshot_id": evolved_snapshot_id,
                "rows": current_rows,
                "files": layout,
            },
            "compatibility": {
                "partition_change_created_data_snapshot": False,
                "old_file_partition_is_null": True,
                "mixed_spec_ids_readable": True,
                "historical_snapshot_preserved": True,
            },
        }
        write_report(report_path, report)
        print(json.dumps(report, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
