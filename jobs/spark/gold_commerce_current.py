"""Opt-in current daily mart from a validated, complete commerce snapshot.

Use one writer. This first version atomically replaces the complete current mart;
it does not claim bounded partition correction or multi-writer scheduling.
"""

from __future__ import annotations

import json
from pathlib import Path

from gold_commerce_daily import summarize, validate_sources
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from spark_catalog import build_session, required_environment

from lakehouse_ops.commerce_snapshot import (
    SnapshotIdentity,
    load_snapshot_identity,
    snapshot_action,
)

TABLE = "lakehouse.gold.commerce_current_daily"
IDENTITY_COLUMNS = ("source_batch_id", "source_batch_at", "source_manifest_sha256")


def publish_snapshot(
    spark: SparkSession,
    daily: DataFrame,
    identity: SnapshotIdentity,
    *,
    table: str = TABLE,
) -> dict[str, str | int]:
    candidate = (
        daily.withColumn("source_batch_at", F.lit(identity.batch_at)).withColumn(
            "source_manifest_sha256", F.lit(identity.manifest_sha256)
        )
    ).cache()
    try:
        if (
            not candidate.count()
            or candidate.filter(
                F.col("order_day").isNull()
                | F.col("source_batch_id").isNull()
                | (F.col("source_batch_id") != identity.batch_id)
            ).count()
        ):
            raise ValueError("current mart requires nonempty daily rows from the selected snapshot")
        if candidate.groupBy("order_day").count().filter(F.col("count") != 1).count():
            raise ValueError("current mart requires one row per order day")
        current = None
        if spark.catalog.tableExists(table):
            target = spark.table(table)
            identities = target.select(*IDENTITY_COLUMNS).distinct().collect()
            if len(identities) != 1:
                raise ValueError("current mart has missing or mixed snapshot identities")
            row = identities[0]
            current = SnapshotIdentity(*[row[column] for column in IDENTITY_COLUMNS])
        action = snapshot_action(current, identity)
        if action == "stale":
            return {"status": "ignored_stale", "batch_id": identity.batch_id}
        if action == "replay":
            target = spark.table(table).select(*candidate.columns)
            if candidate.exceptAll(target).count() or target.exceptAll(candidate).count():
                raise ValueError("replayed snapshot differs from the current mart")
        elif action == "create":
            candidate.writeTo(table).using("iceberg").tableProperty("format-version", "2").create()
        else:
            candidate.writeTo(table).overwrite(F.lit(True))
        target = spark.table(table).select(*candidate.columns)
        if candidate.exceptAll(target).count() or target.exceptAll(candidate).count():
            raise RuntimeError("current snapshot publication post-condition failed")
        return {
            "status": "ready",
            "batch_id": identity.batch_id,
            "action": action,
            "days": target.count(),
        }
    finally:
        candidate.unpersist()


def main() -> None:
    batch_id = required_environment("COMMERCE_BATCH_ID")
    identity = load_snapshot_identity(
        Path(required_environment("COMMERCE_INPUT_ROOT"))
        / "source=commerce"
        / f"batch_id={batch_id}",
        batch_id=batch_id,
        manifest_sha256=required_environment("COMMERCE_MANIFEST_SHA256"),
    )
    spark = build_session("lakehouse-ops-commerce-current-daily")
    spark.sparkContext.setLogLevel("WARN")
    try:
        orders, payments, rejects = validate_sources(spark, batch_id)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
        print(
            json.dumps(
                publish_snapshot(
                    spark,
                    summarize(orders, payments, rejects),
                    identity,
                ),
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
