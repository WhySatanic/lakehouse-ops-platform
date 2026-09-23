from __future__ import annotations

import json

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from spark_catalog import build_session, required_environment


def quality_errors() -> Column:
    candidates = F.array(
        F.when(
            F.col("product_id").isNull() | (F.length(F.trim("product_id")) == 0),
            F.lit("missing_product_id"),
        ),
        F.when(
            F.col("name").isNull() | (F.length(F.trim("name")) == 0),
            F.lit("missing_name"),
        ),
        F.when(
            F.col("category").isNull() | (F.length(F.trim("category")) == 0),
            F.lit("missing_category"),
        ),
        F.when(F.col("unit_price_cents").isNull(), F.lit("missing_unit_price_cents")),
        F.when(
            F.col("unit_price_cents").isNotNull() & (F.col("unit_price_cents") <= 0),
            F.lit("non_positive_unit_price_cents"),
        ),
        F.when(F.col("_product_rank") > 1, F.lit("duplicate_product_id")),
    )
    return F.filter(candidates, lambda error: error.isNotNull())


def classify(source: DataFrame) -> DataFrame:
    product_ids = Window.partitionBy("product_id").orderBy(
        F.col("source_row_hash").desc(),
        F.col("source_row_occurrence").asc(),
    )
    return source.withColumn(
        "_product_rank", F.row_number().over(product_ids)
    ).withColumn("quality_errors", quality_errors())


def create_tables(spark: SparkSession, bucket: str) -> None:
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver "
        f"LOCATION 's3a://{bucket}/warehouse/silver'"
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.commerce_products (
            source_batch_id string,
            source_batch_at timestamp,
            source_row_hash string,
            product_id string,
            name string,
            category string,
            unit_price_cents bigint
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/silver/commerce_products'
        PARTITIONED BY (source_batch_id)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.commerce_product_rejects (
            source_batch_id string,
            source_batch_at timestamp,
            source_row_hash string,
            source_row_occurrence int,
            product_id string,
            name string,
            category string,
            unit_price_cents bigint,
            quality_errors array<string>,
            reject_id string
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/silver/commerce_product_rejects'
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
    bronze = spark.table("lakehouse.bronze.commerce_products").filter(
        F.col("batch_id") == batch_id
    )
    classified = classify(bronze).cache()
    bronze_rows = classified.count()
    if bronze_rows == 0:
        raise RuntimeError(f"selected commerce product batch is empty: {batch_id}")

    valid = (
        classified.filter(F.size("quality_errors") == 0)
        .select(
            F.col("batch_id").alias("source_batch_id"),
            "source_batch_at",
            "source_row_hash",
            "product_id",
            "name",
            "category",
            "unit_price_cents",
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
            "product_id",
            "name",
            "category",
            "unit_price_cents",
            "quality_errors",
            "reject_id",
        )
        .cache()
    )
    valid_rows = valid.count()
    rejected_rows = rejects.count()
    if valid_rows + rejected_rows != bronze_rows:
        raise RuntimeError("commerce product quality counts do not reconcile")

    silver_before = spark.table("lakehouse.silver.commerce_products").filter(
        F.col("source_batch_id") == batch_id
    ).count()
    rejects_before = spark.table(
        "lakehouse.silver.commerce_product_rejects"
    ).filter(F.col("source_batch_id") == batch_id).count()
    valid.createOrReplaceTempView("commerce_product_valid_source")
    spark.sql(
        """
        MERGE INTO lakehouse.silver.commerce_products AS target
        USING commerce_product_valid_source AS source
        ON target.source_batch_id = source.source_batch_id
           AND target.product_id = source.product_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    rejects.createOrReplaceTempView("commerce_product_reject_source")
    spark.sql(
        """
        MERGE INTO lakehouse.silver.commerce_product_rejects AS target
        USING commerce_product_reject_source AS source
        ON target.reject_id = source.reject_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    silver_batch_rows = spark.table("lakehouse.silver.commerce_products").filter(
        F.col("source_batch_id") == batch_id
    ).count()
    reject_batch_rows = spark.table(
        "lakehouse.silver.commerce_product_rejects"
    ).filter(F.col("source_batch_id") == batch_id).count()
    classified.unpersist()
    valid.unpersist()
    rejects.unpersist()
    if (silver_batch_rows, reject_batch_rows) != (valid_rows, rejected_rows):
        raise RuntimeError("commerce product silver post-condition failed")
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
    spark = build_session("lakehouse-ops-silver-commerce-products")
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(write_selected_batch(spark, batch_id=batch_id), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
