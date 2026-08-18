from __future__ import annotations

import json
from pathlib import Path

from spark_catalog import build_session, required_environment

TABLE = "lakehouse.ops.schema_evolution_fixture"


def table_state(spark, *, snapshot_id: int | None = None) -> dict[str, object]:
    if snapshot_id is None:
        frame = spark.table(TABLE)
    else:
        frame = (
            spark.read.format("iceberg")
            .option("snapshot-id", snapshot_id)
            .load(TABLE)
        )
    return {
        "columns": frame.columns,
        "rows": [list(row) for row in frame.orderBy("event_id").collect()],
    }


def current_snapshot_id(spark) -> int:
    row = spark.sql(f"SELECT snapshot_id FROM {TABLE}.refs WHERE name = 'main'").first()
    if row is None:
        raise RuntimeError("table has no main snapshot reference")
    return int(row["snapshot_id"])


def write_report(path: Path, report: dict[str, object]) -> None:
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    report_path = Path(required_environment("SCHEMA_EVOLUTION_REPORT_PATH"))
    spark = build_session("lakehouse-ops-schema-evolution-drill")
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
            LOCATION 's3a://{bucket}/warehouse/ops/schema_evolution_fixture'
            TBLPROPERTIES ('format-version' = '2')
            """
        )
        spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'stable')")
        initial_snapshot_id = current_snapshot_id(spark)
        initial_state = table_state(spark, snapshot_id=initial_snapshot_id)

        spark.sql(
            f"ALTER TABLE {TABLE} ADD COLUMN severity string "
            "COMMENT 'optional incident severity'"
        )
        spark.sql(
            f"INSERT INTO {TABLE} (event_id, payload, severity) "
            "VALUES (2, 'regression', 'warning')"
        )
        evolved_snapshot_id = current_snapshot_id(spark)
        evolved_state = table_state(spark, snapshot_id=evolved_snapshot_id)
        if evolved_snapshot_id == initial_snapshot_id:
            raise RuntimeError("schema evolution append did not create a new snapshot")

        spark.sql(f"ALTER TABLE {TABLE} RENAME COLUMN payload TO message")
        current_id_after_rename = current_snapshot_id(spark)
        current_state = table_state(spark)
        initial_state_after_rename = table_state(spark, snapshot_id=initial_snapshot_id)
        evolved_state_after_rename = table_state(spark, snapshot_id=evolved_snapshot_id)

        expected_initial = {
            "columns": ["event_id", "payload"],
            "rows": [[1, "stable"]],
        }
        expected_evolved = {
            "columns": ["event_id", "payload", "severity"],
            "rows": [[1, "stable", None], [2, "regression", "warning"]],
        }
        expected_current = {
            "columns": ["event_id", "message", "severity"],
            "rows": [[1, "stable", None], [2, "regression", "warning"]],
        }
        if initial_state != expected_initial or initial_state_after_rename != expected_initial:
            raise RuntimeError(
                "initial snapshot schema changed after evolution: "
                f"before={initial_state}, after={initial_state_after_rename}"
            )
        if evolved_state != expected_evolved or evolved_state_after_rename != expected_evolved:
            raise RuntimeError(
                "evolved snapshot schema changed after rename: "
                f"before={evolved_state}, after={evolved_state_after_rename}"
            )
        if current_state != expected_current:
            raise RuntimeError(f"unexpected current schema state: {current_state}")
        if current_id_after_rename != evolved_snapshot_id:
            raise RuntimeError(
                "column rename unexpectedly created a data snapshot: "
                f"before={evolved_snapshot_id}, after={current_id_after_rename}"
            )

        report = {
            "schema_version": "1.0",
            "status": "succeeded",
            "table": TABLE,
            "initial": {
                "snapshot_id": initial_snapshot_id,
                **initial_state_after_rename,
            },
            "after_add": {
                "snapshot_id": evolved_snapshot_id,
                **evolved_state_after_rename,
            },
            "after_rename": {
                "snapshot_id": current_id_after_rename,
                **current_state,
            },
            "compatibility": {
                "added_column_defaults_to_null": True,
                "historical_snapshot_schemas_preserved": True,
                "renamed_values_preserved": True,
                "rename_created_data_snapshot": False,
            },
        }
        write_report(report_path, report)
        print(json.dumps(report, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
