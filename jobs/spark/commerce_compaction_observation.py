"""Bounded fragmentation/compaction of a diagnostic copy, never the source gold table."""

from __future__ import annotations

import json
import os
from pathlib import Path

from lakehouse_ops.iceberg.executor import SparkMaintenanceExecutor

TABLE = "lakehouse.gold.commerce_compaction_daily"


def main() -> None:
    from spark_catalog import build_session, required_environment

    if os.environ.get("COMPACTION_CONFIRMED") != "1":
        raise ValueError("requires explicit diagnostic compaction confirmation")
    mode = required_environment("COMPACTION_MODE")
    if mode not in {"prepare", "compact"}:
        raise ValueError("unsupported compaction observation mode")
    path = Path(required_environment("COMPACTION_REPORT_PATH"))
    spark = build_session("lakehouse-commerce-compaction-observation")
    try:
        spark.sparkContext.setLogLevel("WARN")

        class Executor:
            def query(self, sql):
                return [row.asDict(recursive=True) for row in spark.sql(sql).collect()]

        executor = Executor()
        if mode == "prepare":
            if spark.catalog.tableExists(TABLE):
                raise ValueError("diagnostic target already exists; no overwrite allowed")
            source = spark.table("lakehouse.gold.commerce_daily")
            count = source.count()
            if not 6 <= count <= 100_000:
                raise ValueError("requires 6..100000 source gold rows")
            source.repartition(6).writeTo(TABLE).tableProperty(
                "format-version", "2",
            ).tableProperty("write.distribution-mode", "none").create()
            report = {"status": "ready", "mode": mode, "source_rows": count}
        else:
            plan = json.loads(Path(required_environment("COMPACTION_PLAN_PATH")).read_text())
            if plan["table"] != TABLE:
                raise ValueError("compaction plan must target only the diagnostic table")
            actions = [a for a in plan["actions"] if a["action_type"] == "rewrite_data_files"]
            if len(actions) != 1:
                raise ValueError("requires one diagnostic data-file rewrite action")
            action = actions[0]["action_id"]
            maintenance = SparkMaintenanceExecutor(executor)
            dry = maintenance.run(plan, action)
            applied = maintenance.run(
                plan, action, apply=True, approved_plan_id=plan["plan_id"],
                approved_snapshot_id=required_environment("COMPACTION_EXPECTED_SNAPSHOT"),
            )
            report = {"status": "ready" if applied.status == "succeeded" else "failed",
                      "mode": mode, "plan": plan, "dry_run": dry.as_dict(),
                      "execution": applied.as_dict()}
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, sort_keys=True))
        if report["status"] != "ready":
            raise RuntimeError("diagnostic compaction did not reconcile")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
