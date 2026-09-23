from __future__ import annotations

import json
import os

from pyspark.sql import functions as F
from spark_catalog import build_session


def main() -> None:
    batch_id = os.environ["COMMERCE_BATCH_ID"]
    expected_valid = int(os.environ["EXPECTED_COMMERCE_PRODUCT_VALID_ROWS"])
    expected_rejects = int(os.environ["EXPECTED_COMMERCE_PRODUCT_REJECT_ROWS"])
    spark = build_session("lakehouse-ops-commerce-product-silver-check")
    spark.sparkContext.setLogLevel("WARN")
    try:
        bronze = spark.table("lakehouse.bronze.commerce_products").filter(
            F.col("batch_id") == batch_id
        )
        valid = spark.table("lakehouse.silver.commerce_products").filter(
            F.col("source_batch_id") == batch_id
        )
        rejects = spark.table("lakehouse.silver.commerce_product_rejects").filter(
            F.col("source_batch_id") == batch_id
        )
        bronze_rows = bronze.count()
        valid_rows = valid.count()
        reject_rows = rejects.count()
        invalid_valid_rows = valid.filter(
            F.col("product_id").isNull()
            | (F.length(F.trim("product_id")) == 0)
            | F.col("name").isNull()
            | (F.length(F.trim("name")) == 0)
            | F.col("category").isNull()
            | (F.length(F.trim("category")) == 0)
            | F.col("unit_price_cents").isNull()
            | (F.col("unit_price_cents") <= 0)
        ).count()
        assert valid_rows == expected_valid
        assert reject_rows == expected_rejects
        assert valid_rows + reject_rows == bronze_rows
        assert invalid_valid_rows == 0
        assert valid.groupBy("product_id").count().filter(F.col("count") > 1).count() == 0
        assert rejects.select("reject_id").distinct().count() == reject_rows
        print(
            json.dumps(
                {
                    "status": "ready",
                    "batch_id": batch_id,
                    "bronze_rows": bronze_rows,
                    "valid_rows": valid_rows,
                    "reject_rows": reject_rows,
                    "invalid_valid_rows": invalid_valid_rows,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
