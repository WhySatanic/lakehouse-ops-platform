from __future__ import annotations

import json
import os
from datetime import datetime

from gold_commerce_daily import summarize
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from spark_catalog import build_session


def check_payment_fanout(spark: SparkSession) -> None:
    day = datetime(2026, 1, 2, 12)
    orders = spark.createDataFrame(
        [
            ("synthetic", "one", "customer-a", 100, day, True),
            ("synthetic", "two", "customer-b", 200, day, False),
        ],
        "source_batch_id string, order_id string, customer_id string, "
        "total_cents long, event_at timestamp, is_late boolean",
    )
    payments = spark.createDataFrame(
        [
            ("payment-a", "one", 40, "captured"),
            ("payment-b", "one", 80, "captured"),
            ("payment-c", "one", 10, "declined"),
        ],
        "payment_id string, order_id string, amount_cents long, status string",
    )
    rejects = spark.createDataFrame(
        [("reject-a", "one")], "reject_id string, order_id string"
    )
    row = summarize(orders, payments, rejects).first()
    assert row.order_count == 2
    assert row.buyer_count == 2
    assert row.ordered_amount_cents == 300
    assert row.captured_revenue_cents == 120
    assert row.captured_payment_count == 2
    assert row.noncaptured_payment_count == 1
    assert row.rejected_payment_count == 1
    assert row.missing_payment_count == 1
    assert row.late_order_count == 1


def main() -> None:
    batch_id = os.environ["COMMERCE_BATCH_ID"]
    expected_orders = int(os.environ["EXPECTED_COMMERCE_GOLD_ORDERS"])
    expected_rejects = int(os.environ["EXPECTED_COMMERCE_GOLD_REJECTS"])
    spark = build_session("lakehouse-ops-commerce-daily-gold-check")
    spark.sparkContext.setLogLevel("WARN")
    try:
        check_payment_fanout(spark)
        mart = spark.table("lakehouse.gold.commerce_daily").filter(
            F.col("source_batch_id") == batch_id
        )
        orders = spark.table("lakehouse.silver.commerce_orders").filter(
            F.col("source_batch_id") == batch_id
        )
        payments = spark.table("lakehouse.silver.commerce_payments").filter(
            F.col("source_batch_id") == batch_id
        )
        rejects = spark.table("lakehouse.silver.commerce_payment_rejects").filter(
            F.col("source_batch_id") == batch_id
        )
        totals = mart.agg(
            F.sum("order_count").alias("orders"),
            F.sum("ordered_amount_cents").alias("ordered_cents"),
            F.sum("captured_revenue_cents").alias("revenue_cents"),
            F.sum("captured_payment_count").alias("captured_payments"),
            F.sum("noncaptured_payment_count").alias("noncaptured_payments"),
            F.sum("rejected_payment_count").alias("rejected_payments"),
            F.sum("missing_payment_count").alias("missing_payments"),
            F.sum("late_order_count").alias("late_orders"),
        ).first()
        captured = payments.filter(F.col("status") == "captured")
        assert mart.count() > 0
        assert mart.select("order_day").distinct().count() == mart.count()
        assert mart.filter(F.col("order_day").isNull()).count() == 0
        assert mart.filter(
            (F.col("buyer_count") > F.col("order_count"))
            | (F.col("buyer_count") <= 0)
            | (F.col("captured_revenue_cents") < 0)
        ).count() == 0
        assert totals.orders == expected_orders == orders.count()
        assert totals.ordered_cents == orders.agg(F.sum("total_cents")).first()[0]
        assert totals.revenue_cents == captured.agg(F.sum("amount_cents")).first()[0]
        assert totals.captured_payments == captured.count()
        assert totals.noncaptured_payments == payments.count() - captured.count()
        assert totals.rejected_payments == expected_rejects == rejects.count()
        assert totals.missing_payments == 0
        assert totals.late_orders == orders.filter(F.col("is_late")).count()
        print(
            json.dumps(
                {
                    "status": "ready",
                    "batch_id": batch_id,
                    "days": mart.count(),
                    "orders": totals.orders,
                    "revenue_cents": totals.revenue_cents,
                    "rejected_payments": totals.rejected_payments,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
