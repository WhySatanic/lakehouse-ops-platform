"""Check existing Spark gold aggregation against the related-snapshot oracle."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from gold_commerce_current import TABLE, publish_snapshot
from gold_commerce_daily import summarize
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse_ops.commerce_snapshot import SnapshotIdentity, load_snapshot_identity


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    expected = json.loads((args.root / "expected.json").read_text())
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("commerce-change-scenario")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", "/tmp/commerce-current-warehouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.sql("CREATE NAMESPACE lakehouse.gold")
        revenue = []
        for batch in expected["batches"]:
            root = args.root / f"batch_id={batch['batch_id']}"
            orders = (
                spark.read.json(str(root / "orders.jsonl"))
                .withColumn("source_batch_id", F.lit(batch["batch_id"]))
                .withColumn("event_at", F.to_timestamp("event_at"))
                .withColumn(
                    "is_late", F.datediff(F.to_date("ingested_at"), F.to_date("event_at")) > 30
                )
            )
            payments = spark.read.json(str(root / "payments.jsonl"))
            rejects = spark.createDataFrame([], "reject_id string, order_id string")
            daily_frame = summarize(orders, payments, rejects)
            identity = load_snapshot_identity(
                root,
                batch_id=batch["batch_id"],
                manifest_sha256=hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest(),
            )
            publication = publish_snapshot(spark, daily_frame, identity)
            assert publication["action"] == ("create" if not revenue else "replace")
            daily = spark.table(TABLE).collect()
            revenue.append(sum(row.captured_revenue_cents for row in daily))
            if len(revenue) == 1:
                initial_frame, initial_identity = daily_frame, identity
            if batch == expected["batches"][-1]:
                assert {str(row.order_day): row.captured_revenue_cents for row in daily} == (
                    expected["latest_daily_revenue_cents"]
                )
                assert sum(row.late_order_count for row in daily) == 1
        assert revenue == expected["snapshot_revenue_cents"]
        snapshot_count = spark.table(f"{TABLE}.snapshots").count()
        assert publish_snapshot(spark, daily_frame, identity)["action"] == "replay"
        assert publish_snapshot(spark, initial_frame, initial_identity)["status"] == "ignored_stale"
        assert spark.table(f"{TABLE}.snapshots").count() == snapshot_count
        try:
            publish_snapshot(
                spark, daily_frame.withColumn("captured_revenue_cents", F.lit(0)), identity
            )
        except ValueError as error:
            assert "replayed snapshot differs" in str(error)
        else:
            raise AssertionError("conflicting replay was accepted")
        # A later full snapshot removes a day; overwrite must not leave a stale partition.
        removal = daily_frame.filter(F.col("order_day") != F.lit("2026-01-01")).withColumn(
            "source_batch_id",
            F.lit("d" * 16),
        )
        publish_snapshot(
            spark,
            removal,
            SnapshotIdentity(
                "d" * 16,
                "2026-02-04T00:00:00+00:00",
                "4" * 64,
            ),
        )
        assert spark.table(TABLE).count() == 1
        assert spark.table(TABLE).first().captured_revenue_cents == 1500
        print(
            json.dumps(
                {
                    "status": "passed",
                    "scope": "current_snapshot_iceberg_acceptance",
                    "replay_and_stale_snapshot": "verified",
                    "removed_day": "verified",
                    "snapshot_revenue_cents": revenue,
                    "summed_snapshot_revenue_cents": sum(revenue),
                    "latest_snapshot_revenue_cents": revenue[-1],
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
