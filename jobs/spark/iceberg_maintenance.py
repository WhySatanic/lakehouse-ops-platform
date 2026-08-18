from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from spark_catalog import build_session, required_environment

from lakehouse_ops.iceberg.executor import SparkMaintenanceExecutor


class SparkSqlExecutor:
    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def query(self, sql: str) -> list[dict[str, Any]]:
        return [row.asDict(recursive=True) for row in self._spark.sql(sql).collect()]


def main() -> None:
    plan_path = Path(required_environment("MAINTENANCE_PLAN_PATH"))
    report_path = Path(required_environment("MAINTENANCE_REPORT_PATH"))
    action_id = required_environment("MAINTENANCE_ACTION_ID")
    apply = os.environ.get("MAINTENANCE_APPLY", "false").lower() == "true"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    spark = build_session("lakehouse-ops-iceberg-maintenance")
    spark.sparkContext.setLogLevel("WARN")
    try:
        report = SparkMaintenanceExecutor(SparkSqlExecutor(spark)).run(
            plan,
            action_id,
            apply=apply,
            approved_plan_id=os.environ.get("MAINTENANCE_APPROVED_PLAN_ID"),
            approved_snapshot_id=os.environ.get("MAINTENANCE_APPROVED_SNAPSHOT_ID"),
        )
        report_path.write_text(
            json.dumps(report.as_dict(), sort_keys=True) + "\n", encoding="utf-8"
        )
        print(json.dumps(report.as_dict(), sort_keys=True))
        if report.status == "reconciliation_failed":
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
