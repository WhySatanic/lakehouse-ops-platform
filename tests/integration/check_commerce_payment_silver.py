from __future__ import annotations

import json
import os

from pyspark.sql import functions as F
from spark_catalog import build_session


def main() -> None:
    batch_id = os.environ["COMMERCE_BATCH_ID"]
    expected_valid = int(os.environ["EXPECTED_COMMERCE_PAYMENT_VALID_ROWS"])
    expected_rejects = int(os.environ["EXPECTED_COMMERCE_PAYMENT_REJECT_ROWS"])
    spark = build_session("lakehouse-ops-commerce-payment-silver-check")
    spark.sparkContext.setLogLevel("WARN")
    try:
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
