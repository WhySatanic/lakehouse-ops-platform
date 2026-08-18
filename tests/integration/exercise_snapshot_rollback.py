from __future__ import annotations

import json
from pathlib import Path

from spark_catalog import build_session, required_environment

TABLE = "lakehouse.ops.snapshot_recovery_fixture"


def table_rows(spark, *, snapshot_id: int | None = None) -> list[list[object]]:
    if snapshot_id is None:
        frame = spark.table(TABLE)
    else:
        frame = (
            spark.read.format("iceberg")
            .option("snapshot-id", snapshot_id)
            .load(TABLE)
        )
    return [list(row) for row in frame.orderBy("event_id").collect()]


def write_report(path: Path, report: dict[str, object]) -> None:
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    report_path = Path(required_environment("SNAPSHOT_ROLLBACK_REPORT_PATH"))
    spark = build_session("lakehouse-ops-snapshot-rollback-drill")
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
            LOCATION 's3a://{bucket}/warehouse/ops/snapshot_recovery_fixture'
            TBLPROPERTIES ('format-version' = '2')
            """
        )
        spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'stable')")
        first_snapshots = spark.sql(
            f"SELECT snapshot_id, parent_id FROM {TABLE}.snapshots"
        ).collect()
        if len(first_snapshots) != 1 or first_snapshots[0]["parent_id"] is not None:
            raise RuntimeError("first append did not create the expected root snapshot")
        target_snapshot_id = int(first_snapshots[0]["snapshot_id"])

        spark.sql(f"INSERT INTO {TABLE} VALUES (2, 'regression')")
        snapshots = spark.sql(
            f"SELECT snapshot_id, parent_id FROM {TABLE}.snapshots"
        ).collect()
        later = [row for row in snapshots if int(row["snapshot_id"]) != target_snapshot_id]
        if len(later) != 1 or int(later[0]["parent_id"]) != target_snapshot_id:
            raise RuntimeError("second append did not create the expected child snapshot")
        previous_snapshot_id = int(later[0]["snapshot_id"])

        before_rows = table_rows(spark)
        historical_rows = table_rows(spark, snapshot_id=target_snapshot_id)
        result = spark.sql(
            "CALL lakehouse.system.rollback_to_snapshot("
            "table => 'ops.snapshot_recovery_fixture', "
            f"snapshot_id => {target_snapshot_id})"
        ).first()
        if result is None:
            raise RuntimeError("rollback procedure returned no result")
        rollback = {key: int(value) for key, value in result.asDict().items()}

        after_rows = table_rows(spark)
        abandoned_rows = table_rows(spark, snapshot_id=previous_snapshot_id)
        history = spark.sql(
            f"SELECT snapshot_id, is_current_ancestor FROM {TABLE}.history"
        ).collect()
        current_ancestor_ids = sorted(
            {int(row["snapshot_id"]) for row in history if row["is_current_ancestor"]}
        )
        abandoned_ids = sorted(
            {int(row["snapshot_id"]) for row in history if not row["is_current_ancestor"]}
        )

        expected_rollback = {
            "previous_snapshot_id": previous_snapshot_id,
            "current_snapshot_id": target_snapshot_id,
        }
        if rollback != expected_rollback:
            raise RuntimeError(f"unexpected rollback result: {rollback}")
        if before_rows != [[1, "stable"], [2, "regression"]]:
            raise RuntimeError(f"unexpected pre-rollback rows: {before_rows}")
        if historical_rows != [[1, "stable"]] or after_rows != historical_rows:
            raise RuntimeError(
                "rollback did not restore the historical snapshot: "
                f"historical={historical_rows}, current={after_rows}"
            )
        if abandoned_rows != before_rows:
            raise RuntimeError(f"abandoned snapshot is not readable: {abandoned_rows}")
        if current_ancestor_ids != [target_snapshot_id] or abandoned_ids != [
            previous_snapshot_id
        ]:
            raise RuntimeError(
                "unexpected history lineage after rollback: "
                f"ancestors={current_ancestor_ids}, abandoned={abandoned_ids}"
            )

        report = {
            "schema_version": "1.0",
            "status": "succeeded",
            "table": TABLE,
            "before": {
                "snapshot_id": previous_snapshot_id,
                "rows": before_rows,
            },
            "time_travel": {
                "snapshot_id": target_snapshot_id,
                "rows": historical_rows,
            },
            "rollback": rollback,
            "after": {
                "snapshot_id": target_snapshot_id,
                "rows": after_rows,
            },
            "abandoned_snapshot": {
                "snapshot_id": previous_snapshot_id,
                "readable": True,
                "rows": abandoned_rows,
            },
            "history": {
                "current_ancestor_ids": current_ancestor_ids,
                "abandoned_snapshot_ids": abandoned_ids,
            },
        }
        write_report(report_path, report)
        print(json.dumps(report, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
