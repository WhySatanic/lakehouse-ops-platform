from __future__ import annotations

import json
from pathlib import Path

from spark_catalog import build_session, required_environment

ROWS_PER_DAY = 2_048
DAYS = 32
TOTAL_ROWS = ROWS_PER_DAY * DAYS
TARGET_DAY = "2026-01-16"
UNPARTITIONED = "lakehouse.ops.pruning_unpartitioned"
PARTITIONED = "lakehouse.ops.pruning_partitioned"


def table_state(spark, table: str) -> dict[str, int]:
    snapshot = spark.sql(
        f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'"
    ).first()
    files = spark.sql(
        f"SELECT COUNT(*) AS file_count, SUM(record_count) AS record_count, "
        f"SUM(file_size_in_bytes) AS total_size_bytes FROM {table}.files"
    ).first()
    partitions = spark.sql(
        f"SELECT COUNT(*) AS partition_count FROM {table}.partitions"
    ).first()
    if snapshot is None or files is None or partitions is None:
        raise RuntimeError(f"missing Iceberg metadata for {table}")
    return {
        "snapshot_id": int(snapshot["snapshot_id"]),
        "file_count": int(files["file_count"]),
        "record_count": int(files["record_count"]),
        "total_size_bytes": int(files["total_size_bytes"]),
        "partition_count": int(partitions["partition_count"]),
    }


def filtered_result(spark, table: str) -> dict[str, int]:
    row = spark.sql(
        f"SELECT COUNT(*) AS row_count, SUM(event_id) AS event_id_checksum "
        f"FROM {table} WHERE event_ts >= TIMESTAMP '{TARGET_DAY} 00:00:00' "
        f"AND event_ts < TIMESTAMP '2026-01-17 00:00:00'"
    ).first()
    if row is None:
        raise RuntimeError(f"filtered query returned no result for {table}")
    return {
        "row_count": int(row["row_count"]),
        "event_id_checksum": int(row["event_id_checksum"]),
    }


def write_report(path: Path, report: dict[str, object]) -> None:
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    report_path = Path(required_environment("PARTITION_PRUNING_FIXTURE_REPORT_PATH"))
    spark = build_session("lakehouse-ops-partition-pruning-fixture")
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.ops "
            f"LOCATION 's3a://{bucket}/warehouse/ops'"
        )
        for table in (UNPARTITIONED, PARTITIONED):
            spark.sql(f"DROP TABLE IF EXISTS {table}")

        source = spark.sql(
            f"""
            SELECT
                id AS event_id,
                timestamp_seconds(
                    unix_timestamp(TIMESTAMP '2026-01-01 00:00:00')
                    + CAST(FLOOR(id / {ROWS_PER_DAY}) AS BIGINT) * 86400
                    + pmod(id, {ROWS_PER_DAY})
                ) AS event_ts,
                concat(
                    'event-', lpad(CAST(id AS STRING), 10, '0'), '-',
                    sha2(CAST(id AS STRING), 256)
                ) AS payload
            FROM range({TOTAL_ROWS})
            """
        )
        source.createOrReplaceTempView("partition_pruning_source")

        spark.sql(
            f"""
            CREATE TABLE {UNPARTITIONED} (
                event_id long,
                event_ts timestamp,
                payload string
            )
            USING iceberg
            LOCATION 's3a://{bucket}/warehouse/ops/pruning_unpartitioned'
            TBLPROPERTIES ('format-version' = '2')
            """
        )
        spark.sql(
            f"""
            CREATE TABLE {PARTITIONED} (
                event_id long,
                event_ts timestamp,
                payload string
            )
            USING iceberg
            PARTITIONED BY (days(event_ts))
            LOCATION 's3a://{bucket}/warehouse/ops/pruning_partitioned'
            TBLPROPERTIES ('format-version' = '2')
            """
        )
        spark.sql(f"INSERT INTO {UNPARTITIONED} SELECT * FROM partition_pruning_source")
        spark.sql(f"INSERT INTO {PARTITIONED} SELECT * FROM partition_pruning_source")

        table_states = {
            "unpartitioned": table_state(spark, UNPARTITIONED),
            "partitioned": table_state(spark, PARTITIONED),
        }
        results = {
            "unpartitioned": filtered_result(spark, UNPARTITIONED),
            "partitioned": filtered_result(spark, PARTITIONED),
        }
        if table_states["unpartitioned"]["record_count"] != TOTAL_ROWS:
            raise RuntimeError("unpartitioned fixture has an unexpected record count")
        if table_states["partitioned"]["record_count"] != TOTAL_ROWS:
            raise RuntimeError("partitioned fixture has an unexpected record count")
        if table_states["unpartitioned"]["partition_count"] != 1:
            raise RuntimeError("unpartitioned fixture has an unexpected partition count")
        if table_states["partitioned"]["partition_count"] != DAYS:
            raise RuntimeError("partitioned fixture has an unexpected partition count")
        if results["unpartitioned"] != results["partitioned"]:
            raise RuntimeError("partitioning changed the filtered query result")
        if results["partitioned"]["row_count"] != ROWS_PER_DAY:
            raise RuntimeError("filtered fixture has an unexpected row count")

        report = {
            "schema_version": "1.0",
            "status": "ready",
            "experiment": "iceberg_partition_pruning",
            "target_day": TARGET_DAY,
            "rows_per_day": ROWS_PER_DAY,
            "days": DAYS,
            "total_rows": TOTAL_ROWS,
            "tables": table_states,
            "filtered_result": results["partitioned"],
        }
        write_report(report_path, report)
        print(json.dumps(report, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
