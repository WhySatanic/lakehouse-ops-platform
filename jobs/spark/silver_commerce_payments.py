from __future__ import annotations

import json

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from spark_catalog import build_session, required_environment


def quality_errors() -> Column:
    candidates = F.array(
        F.when(
            F.col("payment_id").isNull() | (F.length(F.trim("payment_id")) == 0),
            F.lit("missing_payment_id"),
        ),
        F.when(
            F.col("order_id").isNull() | (F.length(F.trim("order_id")) == 0),
            F.lit("missing_order_id"),
        ),
        F.when(
            F.col("amount_cents").isNull(),
            F.lit("missing_amount_cents"),
        ),
        F.when(
            F.col("amount_cents").isNotNull() & (F.col("amount_cents") <= 0),
            F.lit("non_positive_amount_cents"),
        ),
        F.when(
            F.col("status").isNull() | (F.length(F.trim("status")) == 0),
            F.lit("missing_status"),
        ),
        F.when(F.col("paid_at").isNull(), F.lit("missing_paid_at")),
        F.when(F.col("_payment_id_count") > 1, F.lit("duplicate_payment_id")),
    )
    return F.filter(candidates, lambda error: error.isNotNull())


def classify(source: DataFrame) -> DataFrame:
    payment_ids = Window.partitionBy("payment_id")
    return source.withColumn("_payment_id_count", F.count(F.lit(1)).over(payment_ids)).withColumn(
        "quality_errors", quality_errors()
    )


def create_tables(spark: SparkSession, bucket: str) -> None:
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver "
        f"LOCATION 's3a://{bucket}/warehouse/silver'"
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.commerce_payments (
            source_batch_id string,
            source_batch_at timestamp,
            source_row_hash string,
            payment_id string,
            order_id string,
            amount_cents bigint,
            status string,
            paid_at timestamp
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/silver/commerce_payments'
        PARTITIONED BY (source_batch_id)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.commerce_payment_rejects (
            source_batch_id string,
            source_batch_at timestamp,
            source_row_hash string,
            source_row_occurrence int,
            payment_id string,
            order_id string,
            amount_cents bigint,
            status string,
            paid_at timestamp,
            quality_errors array<string>,
            reject_id string
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/silver/commerce_payment_rejects'
        PARTITIONED BY (source_batch_id)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )


def write_selected_batch(
    spark: SparkSession, *, batch_id: str
) -> dict[str, int | str]:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    create_tables(spark, bucket)
    bronze = spark.table("lakehouse.bronze.commerce_payments").filter(
        F.col("batch_id") == batch_id
    )
    classified = classify(bronze).cache()
    bronze_rows = classified.count()
    if bronze_rows == 0:
        raise RuntimeError(f"selected commerce payment batch is empty: {batch_id}")

    valid = (
        classified.filter(F.size("quality_errors") == 0)
        .select(
            F.col("batch_id").alias("source_batch_id"),
            "source_batch_at",
            "source_row_hash",
            "payment_id",
            "order_id",
            "amount_cents",
            "status",
            "paid_at",
        )
        .cache()
    )
    rejects = (
        classified.filter(F.size("quality_errors") > 0)
        .withColumn(
            "reject_id",
            F.sha2(
                F.concat_ws(
                    ":",
                    "batch_id",
                    "source_row_hash",
                    F.col("source_row_occurrence").cast("string"),
                ),
                256,
            ),
        )
        .select(
            F.col("batch_id").alias("source_batch_id"),
            "source_batch_at",
            "source_row_hash",
            "source_row_occurrence",
            "payment_id",
            "order_id",
            "amount_cents",
            "status",
            "paid_at",
            "quality_errors",
            "reject_id",
        )
        .cache()
    )
    valid_rows = valid.count()
    rejected_rows = rejects.count()
    if valid_rows + rejected_rows != bronze_rows:
        raise RuntimeError("commerce payment quality counts do not reconcile")

    silver_before = spark.table("lakehouse.silver.commerce_payments").filter(
        F.col("source_batch_id") == batch_id
    ).count()
    rejects_before = spark.table(
        "lakehouse.silver.commerce_payment_rejects"
    ).filter(F.col("source_batch_id") == batch_id).count()

    valid.createOrReplaceTempView("commerce_payment_valid_source")
    spark.sql(
        """
        MERGE INTO lakehouse.silver.commerce_payments AS target
        USING commerce_payment_valid_source AS source
        ON target.source_batch_id = source.source_batch_id
           AND target.payment_id = source.payment_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    rejects.createOrReplaceTempView("commerce_payment_reject_source")
    spark.sql(
        """
        MERGE INTO lakehouse.silver.commerce_payment_rejects AS target
        USING commerce_payment_reject_source AS source
        ON target.reject_id = source.reject_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    silver_batch_rows = spark.table("lakehouse.silver.commerce_payments").filter(
        F.col("source_batch_id") == batch_id
    ).count()
    reject_batch_rows = spark.table(
        "lakehouse.silver.commerce_payment_rejects"
    ).filter(F.col("source_batch_id") == batch_id).count()
    classified.unpersist()
    valid.unpersist()
    rejects.unpersist()
    if (silver_batch_rows, reject_batch_rows) != (valid_rows, rejected_rows):
        raise RuntimeError("commerce payment silver post-condition failed")
    return {
        "status": "ready",
        "batch_id": batch_id,
        "bronze_rows": bronze_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "silver_rows_before": silver_before,
        "silver_batch_rows": silver_batch_rows,
        "silver_rows_inserted": silver_batch_rows - silver_before,
        "reject_rows_before": rejects_before,
        "reject_batch_rows": reject_batch_rows,
        "reject_rows_inserted": reject_batch_rows - rejects_before,
    }


def main() -> None:
    batch_id = required_environment("COMMERCE_BATCH_ID")
    spark = build_session("lakehouse-ops-silver-commerce-payments")
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(write_selected_batch(spark, batch_id=batch_id), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
