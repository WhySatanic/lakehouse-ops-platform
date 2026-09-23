from __future__ import annotations

import json
import os

from pyspark.sql import functions as F
from spark_catalog import build_session


def main() -> None:
    batch_id = os.environ["COMMERCE_BATCH_ID"]
    expected_valid = int(os.environ["EXPECTED_COMMERCE_ORDER_VALID_ROWS"])
    expected_rejects = int(os.environ["EXPECTED_COMMERCE_ORDER_REJECT_ROWS"])
    expected_late = int(os.environ["EXPECTED_COMMERCE_ORDER_LATE_ROWS"])
    spark = build_session("lakehouse-ops-commerce-order-silver-check")
    spark.sparkContext.setLogLevel("WARN")
    try:
        bronze = spark.table("lakehouse.bronze.commerce_orders").filter(
            F.col("batch_id") == batch_id
        )
        valid = spark.table("lakehouse.silver.commerce_orders").filter(
            F.col("source_batch_id") == batch_id
        )
        rejects = spark.table("lakehouse.silver.commerce_order_rejects").filter(
            F.col("source_batch_id") == batch_id
        )
        bronze_rows = bronze.count()
        valid_rows = valid.count()
        reject_rows = rejects.count()
        late_rows = valid.filter(F.col("is_late")).count()
        duplicate_rejects = rejects.filter(
            F.array_contains("quality_errors", "duplicate_order_id")
        ).count()
        customer_ids = (
            spark.table("lakehouse.bronze.commerce_customers")
            .filter(F.col("batch_id") == batch_id)
            .select("customer_id")
            .distinct()
        )
        product_ids = (
            spark.table("lakehouse.bronze.commerce_products")
            .filter(F.col("batch_id") == batch_id)
            .select("product_id")
            .distinct()
        )
        unknown_customer_rows = valid.join(
            customer_ids, "customer_id", "left_anti"
        ).count()
        unknown_product_rows = valid.join(product_ids, "product_id", "left_anti").count()
        assert valid_rows == expected_valid
        assert reject_rows == expected_rejects
        assert late_rows == expected_late
        assert duplicate_rejects == expected_rejects
        assert valid_rows + reject_rows == bronze_rows
        assert valid.groupBy("order_id").count().filter(F.col("count") > 1).count() == 0
        assert rejects.select("reject_id").distinct().count() == reject_rows
        assert unknown_customer_rows == 0
        assert unknown_product_rows == 0
        print(
            json.dumps(
                {
                    "status": "ready",
                    "batch_id": batch_id,
                    "bronze_rows": bronze_rows,
                    "valid_rows": valid_rows,
                    "reject_rows": reject_rows,
                    "late_rows": late_rows,
                    "duplicate_rejects": duplicate_rejects,
                    "unknown_customer_rows": unknown_customer_rows,
                    "unknown_product_rows": unknown_product_rows,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
