from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import functions as F
from spark_catalog import build_session, required_environment

TOTAL_ROWS = 65_536
FILES = 16
RANGE_START = 30_000
RANGE_SIZE = 128
BASELINE = "lakehouse.ops.sort_baseline"
SORTED = "lakehouse.ops.sort_ordered"


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
        f"FROM {table} WHERE event_id >= {RANGE_START} "
        f"AND event_id < {RANGE_START + RANGE_SIZE}"
    ).first()
    if row is None:
        raise RuntimeError(f"filtered query returned no result for {table}")
    return {
        "row_count": int(row["row_count"]),
        "event_id_checksum": int(row["event_id_checksum"]),
    }


def main() -> None:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    report_path = Path(required_environment("SORT_ORDER_FIXTURE_REPORT_PATH"))
    spark = build_session("lakehouse-ops-sort-order-fixture")
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.shuffle.partitions", str(FILES))
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.ops "
            f"LOCATION 's3a://{bucket}/warehouse/ops'"
        )
        for table in (BASELINE, SORTED):
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            spark.sql(
                f"""
                CREATE TABLE {table} (
                    event_id long,
                    event_ts timestamp,
                    payload string
                )
                USING iceberg
                LOCATION 's3a://{bucket}/warehouse/ops/{table.rsplit('.', 1)[-1]}'
                TBLPROPERTIES (
                    'format-version' = '2',
                    'write.target-file-size-bytes' = '2097152'
                )
                """
            )
        spark.sql(
            f"ALTER TABLE {BASELINE} SET TBLPROPERTIES ('write.distribution-mode' = 'none')"
        )
        spark.sql(f"ALTER TABLE {SORTED} WRITE ORDERED BY event_id")

        source = spark.sql(
            f"""
            SELECT
                id AS event_id,
                timestamp_seconds(
                    unix_timestamp(TIMESTAMP '2026-01-01 00:00:00') + id
                ) AS event_ts,
                repeat(sha2(CAST(id AS STRING), 256), 8) AS payload
            FROM range({TOTAL_ROWS})
            """
        )
        baseline_source = source.repartition(
            FILES, F.pmod(F.xxhash64("event_id"), F.lit(FILES))
        )
        baseline_source.writeTo(BASELINE).append()
        source.writeTo(SORTED).append()

        tables = {
            "baseline": table_state(spark, BASELINE),
            "sorted": table_state(spark, SORTED),
        }
        results = {
            "baseline": filtered_result(spark, BASELINE),
            "sorted": filtered_result(spark, SORTED),
        }
        if any(table["record_count"] != TOTAL_ROWS for table in tables.values()):
            raise RuntimeError("sort fixture has an unexpected record count")
        if any(table["partition_count"] != 1 for table in tables.values()):
            raise RuntimeError("sort fixture tables must be unpartitioned")
        if any(table["file_count"] < 8 for table in tables.values()):
            raise RuntimeError("sort fixture did not create enough files")
        if results["baseline"] != results["sorted"]:
            raise RuntimeError("sort order changed the filtered query result")
        if results["sorted"]["row_count"] != RANGE_SIZE:
            raise RuntimeError("sort fixture returned an unexpected row count")

        report = {
            "schema_version": "1.0",
            "status": "ready",
            "experiment": "iceberg_sort_order",
            "total_rows": TOTAL_ROWS,
            "target_files": FILES,
            "range": {
                "start": RANGE_START,
                "end_exclusive": RANGE_START + RANGE_SIZE,
                "size": RANGE_SIZE,
            },
            "tables": tables,
            "filtered_result": results["sorted"],
        }
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(json.dumps(report, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
