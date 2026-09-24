from __future__ import annotations

import json

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from spark_catalog import build_session, required_environment

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


def quality_errors() -> Column:
    candidates = F.array(
        F.when(
            F.col("customer_id").isNull() | (F.length(F.trim("customer_id")) == 0),
            F.lit("missing_customer_id"),
        ),
        F.when(
            F.col("full_name").isNull() | (F.length(F.trim("full_name")) == 0),
            F.lit("missing_full_name"),
        ),
        F.when(F.col("registered_at").isNull(), F.lit("missing_registered_at")),
        F.when(
            F.col("email").isNotNull() & ~F.col("email").rlike(EMAIL_PATTERN),
            F.lit("invalid_email"),
        ),
        F.when(F.col("_customer_rank") > 1, F.lit("duplicate_customer_id")),
    )
    return F.filter(candidates, lambda error: error.isNotNull())


def classify(source: DataFrame) -> DataFrame:
    customer_ids = Window.partitionBy("customer_id").orderBy(
        F.col("source_row_hash").desc(),
        F.col("source_row_occurrence").asc(),
    )
    return source.withColumn(
        "_customer_rank", F.row_number().over(customer_ids)
    ).withColumn("quality_errors", quality_errors())


def create_tables(spark: SparkSession, bucket: str) -> None:
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver "
        f"LOCATION 's3a://{bucket}/warehouse/silver'"
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.commerce_customers (
            source_batch_id string,
            source_batch_at timestamp,
            source_row_hash string,
            customer_id string,
            full_name string,
            email string,
            email_is_missing boolean,
            registered_at timestamp
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/silver/commerce_customers'
        PARTITIONED BY (source_batch_id)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.commerce_customer_rejects (
            source_batch_id string,
            source_batch_at timestamp,
            source_row_hash string,
            source_row_occurrence int,
            customer_id string,
            full_name string,
            email string,
            registered_at timestamp,
            quality_errors array<string>,
            reject_id string
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/silver/commerce_customer_rejects'
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
    bronze = spark.table("lakehouse.bronze.commerce_customers").filter(
        F.col("batch_id") == batch_id
    )
    classified = classify(bronze).cache()
    bronze_rows = classified.count()
    if bronze_rows == 0:
        raise RuntimeError(f"selected commerce customer batch is empty: {batch_id}")

    valid = (
        classified.filter(F.size("quality_errors") == 0)
        .select(
            F.col("batch_id").alias("source_batch_id"),
            "source_batch_at",
            "source_row_hash",
            "customer_id",
            "full_name",
            "email",
            F.col("email").isNull().alias("email_is_missing"),
            "registered_at",
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
            "customer_id",
            "full_name",
            "email",
            "registered_at",
            "quality_errors",
            "reject_id",
        )
        .cache()
    )
    valid_rows = valid.count()
    rejected_rows = rejects.count()
    missing_email_rows = valid.filter(F.col("email_is_missing")).count()
    if valid_rows + rejected_rows != bronze_rows:
        raise RuntimeError("commerce customer quality counts do not reconcile")

    silver_before = spark.table("lakehouse.silver.commerce_customers").filter(
        F.col("source_batch_id") == batch_id
    ).count()
    rejects_before = spark.table(
        "lakehouse.silver.commerce_customer_rejects"
    ).filter(F.col("source_batch_id") == batch_id).count()
    valid.createOrReplaceTempView("commerce_customer_valid_source")
    spark.sql(
        """
        MERGE INTO lakehouse.silver.commerce_customers AS target
        USING commerce_customer_valid_source AS source
        ON target.source_batch_id = source.source_batch_id
           AND target.customer_id = source.customer_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    rejects.createOrReplaceTempView("commerce_customer_reject_source")
    spark.sql(
        """
        MERGE INTO lakehouse.silver.commerce_customer_rejects AS target
        USING commerce_customer_reject_source AS source
        ON target.reject_id = source.reject_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    silver_batch_rows = spark.table("lakehouse.silver.commerce_customers").filter(
        F.col("source_batch_id") == batch_id
    ).count()
    reject_batch_rows = spark.table(
        "lakehouse.silver.commerce_customer_rejects"
    ).filter(F.col("source_batch_id") == batch_id).count()
    classified.unpersist()
    valid.unpersist()
    rejects.unpersist()
    if (silver_batch_rows, reject_batch_rows) != (valid_rows, rejected_rows):
        raise RuntimeError("commerce customer silver post-condition failed")
    return {
        "status": "ready",
        "batch_id": batch_id,
        "bronze_rows": bronze_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "missing_email_rows": missing_email_rows,
        "silver_rows_before": silver_before,
        "silver_batch_rows": silver_batch_rows,
        "silver_rows_inserted": silver_batch_rows - silver_before,
        "reject_rows_before": rejects_before,
        "reject_batch_rows": reject_batch_rows,
        "reject_rows_inserted": reject_batch_rows - rejects_before,
    }


def main() -> None:
    batch_id = required_environment("COMMERCE_BATCH_ID")
    spark = build_session("lakehouse-ops-silver-commerce-customers")
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(write_selected_batch(spark, batch_id=batch_id), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
