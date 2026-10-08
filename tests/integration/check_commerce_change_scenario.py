"""Check existing Spark gold aggregation against the related-snapshot oracle."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from gold_commerce_daily import summarize
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
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
            daily = summarize(orders, payments, rejects).collect()
            revenue.append(sum(row.captured_revenue_cents for row in daily))
            if batch == expected["batches"][-1]:
                assert {str(row.order_day): row.captured_revenue_cents for row in daily} == (
                    expected["latest_daily_revenue_cents"]
                )
                assert sum(row.late_order_count for row in daily) == 1
        assert revenue == expected["snapshot_revenue_cents"]
        print(
            json.dumps(
                {
                    "status": "passed",
                    "scope": "batch_scoped_spark_gold_acceptance",
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
