from __future__ import annotations

import json
import os

from pyspark.sql import functions as F
from spark_catalog import build_session


def main() -> None:
    source_batch_id = os.environ["COMMERCE_BATCH_ID"]
    history_batch_id = os.environ["COMMERCE_HISTORY_BATCH_ID"]
    spark = build_session("lakehouse-ops-commerce-customer-history-seed")
    spark.sparkContext.setLogLevel("WARN")
    try:
        source = spark.table("lakehouse.silver.commerce_customers").filter(
            F.col("source_batch_id") == source_batch_id
        )
        source_rows = source.count()
        if source_rows == 0:
            raise RuntimeError("customer history seed source is empty")
        changed_customer_id = source.select(F.min("customer_id")).first()[0]
        history = (
            source.withColumn("source_batch_id", F.lit(history_batch_id))
            .withColumn("source_batch_at", F.expr("source_batch_at + INTERVAL 1 DAY"))
            .withColumn(
                "email",
                F.when(
                    F.col("customer_id") == changed_customer_id,
                    F.concat(F.lit("history."), F.col("customer_id"), F.lit("@example.test")),
                ).otherwise(F.col("email")),
            )
            .withColumn("email_is_missing", F.col("email").isNull())
            .withColumn(
                "source_row_hash",
                F.sha2(
                    F.to_json(
                        F.struct(
                            "customer_id",
                            "full_name",
                            "email",
                            "email_is_missing",
                            "registered_at",
                        ),
                        {"ignoreNullFields": "false"},
                    ),
                    256,
                ),
            )
        )
        before = spark.table("lakehouse.silver.commerce_customers").filter(
            F.col("source_batch_id") == history_batch_id
        ).count()
        history.createOrReplaceTempView("commerce_customer_history_seed")
        spark.sql(
            """
            MERGE INTO lakehouse.silver.commerce_customers AS target
            USING commerce_customer_history_seed AS source
            ON target.source_batch_id = source.source_batch_id
               AND target.customer_id = source.customer_id
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        after = spark.table("lakehouse.silver.commerce_customers").filter(
            F.col("source_batch_id") == history_batch_id
        ).count()
        if after != source_rows:
            raise RuntimeError("customer history seed post-condition failed")
        print(
            json.dumps(
                {
                    "status": "ready",
                    "source_batch_id": source_batch_id,
                    "history_batch_id": history_batch_id,
                    "changed_customer_id": changed_customer_id,
                    "history_rows": after,
                    "rows_inserted": after - before,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
