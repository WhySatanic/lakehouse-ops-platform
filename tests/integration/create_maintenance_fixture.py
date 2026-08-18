from __future__ import annotations

import json

from spark_catalog import build_session, required_environment


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
        for event_id in range(4):
            spark.sql(
                "INSERT INTO lakehouse.ops.maintenance_fixture VALUES "
                f"({event_id}, 'fixture-{event_id}')"
            )
        files = spark.sql(
            "SELECT count(*) AS count FROM lakehouse.ops.maintenance_fixture.data_files"
        ).first()["count"]
        manifests = spark.sql(
            "SELECT count(*) AS count FROM lakehouse.ops.maintenance_fixture.manifests"
        ).first()["count"]
        rows = spark.table("lakehouse.ops.maintenance_fixture").count()
        if files != 4 or manifests != 4 or rows != 4:
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
