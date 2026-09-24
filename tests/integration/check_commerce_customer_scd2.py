from __future__ import annotations

import json
import os

from pyspark.sql import Window
from pyspark.sql import functions as F
from spark_catalog import build_session


def main() -> None:
    batch_id = os.environ["COMMERCE_BATCH_ID"]
    expected_rows = int(os.environ["EXPECTED_COMMERCE_CUSTOMER_HISTORY_ROWS"])
    expected_current = int(os.environ["EXPECTED_COMMERCE_CUSTOMER_CURRENT_ROWS"])
    expected_versioned = int(os.environ["EXPECTED_COMMERCE_CUSTOMER_VERSIONED_ROWS"])
    spark = build_session("lakehouse-ops-commerce-customer-scd2-check")
    spark.sparkContext.setLogLevel("WARN")
    try:
        source = spark.table("lakehouse.silver.commerce_customers").filter(
            F.col("source_batch_id") == batch_id
        ).withColumn(
            "expected_hash",
            F.sha2(
                F.to_json(
                    F.struct(
                        "full_name", "email", "email_is_missing", "registered_at"
                    ),
                    {"ignoreNullFields": "false"},
                ),
                256,
            ),
        )
        history = spark.table("lakehouse.gold.dim_customers_scd2")
        current = history.filter(F.col("is_current"))
        history_rows = history.count()
        current_rows = current.count()
        historical_rows = history.filter(~F.col("is_current")).count()
        versioned_customers = (
            history.groupBy("customer_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        duplicate_current = (
            current.groupBy("customer_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        invalid_periods = history.filter(
            (F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from")))
            | (F.col("is_current") & F.col("valid_to").isNotNull())
            | (~F.col("is_current") & F.col("valid_to").isNull())
        ).count()
        ordered = Window.partitionBy("customer_id").orderBy("valid_from")
        discontinuous_periods = (
            history.withColumn("previous_valid_to", F.lag("valid_to").over(ordered))
            .filter(
                F.col("previous_valid_to").isNotNull()
                & (F.col("previous_valid_to") != F.col("valid_from"))
            )
            .count()
        )
        current_mismatches = (
            source.alias("source")
            .join(
                current.alias("current"),
                F.col("source.customer_id") == F.col("current.customer_id"),
                "full",
            )
            .filter(
                F.col("source.customer_id").isNull()
                | F.col("current.customer_id").isNull()
                | (F.col("source.expected_hash") != F.col("current.attribute_hash"))
            )
            .count()
        )
        assert history_rows == expected_rows
        assert current_rows == expected_current
        assert historical_rows == expected_rows - expected_current
        assert versioned_customers == expected_versioned
        assert duplicate_current == 0
        assert invalid_periods == 0
        assert discontinuous_periods == 0
        assert current_mismatches == 0
        assert history.select("customer_version_id").distinct().count() == history_rows
        print(
            json.dumps(
                {
                    "status": "ready",
                    "batch_id": batch_id,
                    "history_rows": history_rows,
                    "current_rows": current_rows,
                    "historical_rows": historical_rows,
                    "versioned_customers": versioned_customers,
                    "invalid_periods": invalid_periods,
                    "discontinuous_periods": discontinuous_periods,
                    "current_mismatches": current_mismatches,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
