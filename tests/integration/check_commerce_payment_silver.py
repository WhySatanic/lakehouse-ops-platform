from __future__ import annotations

import json
import os
from datetime import datetime

from gold_commerce_daily import summarize
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from silver_commerce_payments import classify
from spark_catalog import build_session


def check_payment_states(spark: SparkSession) -> None:
    """Literal status/revenue oracle, also run by the ordinary payment silver CI check."""
    stamp = datetime(2026, 2, 1, 12)
    states = [
        ("captured", []), ("declined", []), ("pending", []),
        ("failed", []), ("cancelled", []), ("refunded", []),
        ("capturedd", ["unknown_status"]), ("CAPTURED", ["unknown_status"]),
        (" captured ", ["unknown_status"]), ("partially_refunded", ["unknown_status"]),
        (None, ["missing_status"]), ("   ", ["missing_status"]),
    ]
    source = spark.createDataFrame(
        [(f"p{i}", "order", 125, state, stamp) for i, (state, _) in enumerate(states)],
        "payment_id string, order_id string, amount_cents long, status string, paid_at timestamp",
    )
    observed = {row.payment_id: row.quality_errors for row in classify(source).collect()}
    assert observed == {f"p{i}": errors for i, (_, errors) in enumerate(states)}, observed
    orders = spark.createDataFrame(
        [("snapshot", "order", "customer", 125, stamp, False)],
        "source_batch_id string, order_id string, customer_id string, "
        "total_cents long, event_at timestamp, is_late boolean",
    )
    rejects = spark.createDataFrame([], "reject_id string, order_id string")
    # Same payment identity in successive complete snapshots, not additive refund events.
    for state, revenue in [("captured", 125), ("refunded", 0), ("cancelled", 0)]:
        payment = source.filter(F.col("payment_id") == "p0").withColumn("status", F.lit(state))
        valid = classify(payment).filter(F.size("quality_errors") == 0)
        row = summarize(orders, valid, rejects).first()
        assert row.captured_revenue_cents == revenue, (state, row)
        assert row.captured_payment_count == int(state == "captured")
        assert row.noncaptured_payment_count == int(state != "captured")
        assert row.order_count == 1 and row.ordered_amount_cents == 125
        assert row.missing_payment_count == row.rejected_payment_count == 0


def main() -> None:
    batch_id = os.environ["COMMERCE_BATCH_ID"]
    expected_valid = int(os.environ["EXPECTED_COMMERCE_PAYMENT_VALID_ROWS"])
    expected_rejects = int(os.environ["EXPECTED_COMMERCE_PAYMENT_REJECT_ROWS"])
    spark = build_session("lakehouse-ops-commerce-payment-silver-check")
    spark.sparkContext.setLogLevel("WARN")
    try:
        check_payment_states(spark)
        valid = spark.table("lakehouse.silver.commerce_payments").filter(
            F.col("source_batch_id") == batch_id
        )
        rejects = spark.table("lakehouse.silver.commerce_payment_rejects").filter(
            F.col("source_batch_id") == batch_id
        )
        valid_rows = valid.count()
        reject_rows = rejects.count()
        assert valid_rows == expected_valid
        assert reject_rows == expected_rejects
        assert valid.groupBy("payment_id").count().filter(F.col("count") > 1).count() == 0
        invalid_valid_rows = valid.filter(
            F.col("amount_cents").isNull() | (F.col("amount_cents") <= 0)
        ).count()
        assert invalid_valid_rows == 0
        missing_amount_rejects = rejects.filter(
            F.array_contains("quality_errors", "missing_amount_cents")
        ).count()
        assert missing_amount_rejects == expected_rejects
        print(
            json.dumps(
                {
                    "status": "ready",
                    "batch_id": batch_id,
                    "valid_rows": valid_rows,
                    "reject_rows": reject_rows,
                    "missing_amount_rejects": missing_amount_rejects,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
