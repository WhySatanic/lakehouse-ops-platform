from __future__ import annotations

import json

from spark_catalog import build_session, required_environment

FILES = 4
ROWS_PER_FILE = 25_000


def main() -> None:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    spark = build_session("lakehouse-ops-maintenance-fixture")
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.sql(
            "CREATE NAMESPACE IF NOT EXISTS lakehouse.ops "
            f"LOCATION 's3a://{bucket}/warehouse/ops'"
        )
        spark.sql("DROP TABLE IF EXISTS lakehouse.ops.maintenance_fixture")
        spark.sql(
            f"""
            CREATE TABLE lakehouse.ops.maintenance_fixture (
                event_id long,
                payload string
            )
            USING iceberg
            LOCATION 's3a://{bucket}/warehouse/ops/maintenance_fixture'
            TBLPROPERTIES (
                'format-version' = '2',
                'write.target-file-size-bytes' = '134217728'
            )
            """
        )
        for batch in range(FILES):
            first_id = batch * ROWS_PER_FILE
            last_id = first_id + ROWS_PER_FILE
            (
                spark.range(first_id, last_id, numPartitions=1)
                .selectExpr(
                    "id AS event_id",
                    "concat('fixture-', cast(id AS string)) AS payload",
                )
                .writeTo("lakehouse.ops.maintenance_fixture")
                .append()
            )
        files = spark.sql(
            "SELECT count(*) AS count FROM lakehouse.ops.maintenance_fixture.data_files"
        ).first()["count"]
        manifests = spark.sql(
            "SELECT count(*) AS count FROM lakehouse.ops.maintenance_fixture.manifests"
        ).first()["count"]
        rows = spark.table("lakehouse.ops.maintenance_fixture").count()
        if files != FILES or manifests != FILES or rows != FILES * ROWS_PER_FILE:
            raise RuntimeError(
                f"unexpected fixture state: files={files}, manifests={manifests}, rows={rows}"
            )
        print(
            json.dumps(
                {
                    "status": "ready",
                    "files": files,
                    "manifests": manifests,
                    "rows": rows,
                }
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
