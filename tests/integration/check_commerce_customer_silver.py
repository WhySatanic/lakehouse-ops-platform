from __future__ import annotations

import json
import os

from pyspark.sql import functions as F
from spark_catalog import build_session

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


def main() -> None:
    batch_id = os.environ["COMMERCE_BATCH_ID"]
    expected_valid = int(os.environ["EXPECTED_COMMERCE_CUSTOMER_VALID_ROWS"])
    expected_rejects = int(os.environ["EXPECTED_COMMERCE_CUSTOMER_REJECT_ROWS"])
    expected_missing_email = int(
        os.environ["EXPECTED_COMMERCE_CUSTOMER_MISSING_EMAIL_ROWS"]
    )
    spark = build_session("lakehouse-ops-commerce-customer-silver-check")
    spark.sparkContext.setLogLevel("WARN")
    try:
        bronze = spark.table("lakehouse.bronze.commerce_customers").filter(
            F.col("batch_id") == batch_id
        )
        valid = spark.table("lakehouse.silver.commerce_customers").filter(
            F.col("source_batch_id") == batch_id
        )
        rejects = spark.table("lakehouse.silver.commerce_customer_rejects").filter(
            F.col("source_batch_id") == batch_id
        )
        bronze_rows = bronze.count()
        valid_rows = valid.count()
        reject_rows = rejects.count()
        missing_email_rows = valid.filter(F.col("email_is_missing")).count()
        invalid_valid_rows = valid.filter(
            F.col("customer_id").isNull()
            | (F.length(F.trim("customer_id")) == 0)
            | F.col("full_name").isNull()
            | (F.length(F.trim("full_name")) == 0)
            | F.col("registered_at").isNull()
            | (F.col("email").isNotNull() & ~F.col("email").rlike(EMAIL_PATTERN))
            | (F.col("email_is_missing") != F.col("email").isNull())
        ).count()
        assert valid_rows == expected_valid
        assert reject_rows == expected_rejects
        assert missing_email_rows == expected_missing_email
        assert valid_rows + reject_rows == bronze_rows
        assert invalid_valid_rows == 0
        assert valid.groupBy("customer_id").count().filter(F.col("count") > 1).count() == 0
        assert rejects.select("reject_id").distinct().count() == reject_rows
        print(
            json.dumps(
                {
                    "status": "ready",
                    "batch_id": batch_id,
                    "bronze_rows": bronze_rows,
                    "valid_rows": valid_rows,
                    "reject_rows": reject_rows,
                    "missing_email_rows": missing_email_rows,
                    "invalid_valid_rows": invalid_valid_rows,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
