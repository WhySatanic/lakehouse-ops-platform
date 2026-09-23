from __future__ import annotations

import json

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from spark_catalog import build_session, required_environment


def quality_errors() -> Column:
    candidates = F.array(
        F.when(
            F.col("order_id").isNull() | (F.length(F.trim("order_id")) == 0),
            F.lit("missing_order_id"),
        ),
        F.when(
            F.col("customer_id").isNull() | (F.length(F.trim("customer_id")) == 0),
            F.lit("missing_customer_id"),
        ),
        F.when(
            F.col("product_id").isNull() | (F.length(F.trim("product_id")) == 0),
            F.lit("missing_product_id"),
        ),
        F.when(F.col("quantity").isNull(), F.lit("missing_quantity")),
        F.when(
            F.col("quantity").isNotNull() & (F.col("quantity") <= 0),
            F.lit("non_positive_quantity"),
        ),
        F.when(F.col("unit_price_cents").isNull(), F.lit("missing_unit_price_cents")),
        F.when(
            F.col("unit_price_cents").isNotNull() & (F.col("unit_price_cents") <= 0),
            F.lit("non_positive_unit_price_cents"),
        ),
        F.when(F.col("total_cents").isNull(), F.lit("missing_total_cents")),
        F.when(
            F.col("total_cents").isNotNull() & (F.col("total_cents") <= 0),
            F.lit("non_positive_total_cents"),
        ),
        F.when(
            F.col("quantity").isNotNull()
            & F.col("unit_price_cents").isNotNull()
            & F.col("total_cents").isNotNull()
            & (F.col("total_cents") != F.col("quantity") * F.col("unit_price_cents")),
            F.lit("total_mismatch"),
        ),
        F.when(F.col("event_at").isNull(), F.lit("missing_event_at")),
        F.when(F.col("ingested_at").isNull(), F.lit("missing_ingested_at")),
        F.when(F.col("known_customer_id").isNull(), F.lit("unknown_customer_id")),
        F.when(F.col("known_product_id").isNull(), F.lit("unknown_product_id")),
        F.when(F.col("_order_rank") > 1, F.lit("duplicate_order_id")),
    )
    return F.filter(candidates, lambda error: error.isNotNull())


def classify(spark: SparkSession, source: DataFrame, *, batch_id: str) -> DataFrame:
    customers = (
        spark.table("lakehouse.bronze.commerce_customers")
        .filter(F.col("batch_id") == batch_id)
        .select(F.col("customer_id").alias("known_customer_id"))
        .distinct()
    )
    products = (
        spark.table("lakehouse.bronze.commerce_products")
        .filter(F.col("batch_id") == batch_id)
        .select(F.col("product_id").alias("known_product_id"))
        .distinct()
    )
    order_ids = Window.partitionBy("order_id").orderBy(
        F.col("ingested_at").desc_nulls_last(),
        F.col("source_row_hash").desc(),
        F.col("source_row_occurrence").asc(),
    )
    enriched = (
        source.withColumn("_order_rank", F.row_number().over(order_ids))
        .join(
            customers,
            F.col("customer_id") == F.col("known_customer_id"),
            "left",
        )
        .join(
            products,
            F.col("product_id") == F.col("known_product_id"),
            "left",
        )
        .withColumn(
            "is_late",
            F.expr("event_at < source_batch_at - INTERVAL 30 DAYS"),
        )
    )
    return enriched.withColumn("quality_errors", quality_errors())


def create_tables(spark: SparkSession, bucket: str) -> None:
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver "
        f"LOCATION 's3a://{bucket}/warehouse/silver'"
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.commerce_orders (
            source_batch_id string,
            source_batch_at timestamp,
            source_row_hash string,
            order_id string,
            customer_id string,
            product_id string,
            quantity int,
            unit_price_cents bigint,
            total_cents bigint,
            event_at timestamp,
            ingested_at timestamp,
            is_late boolean
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/silver/commerce_orders'
        PARTITIONED BY (source_batch_id)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.commerce_order_rejects (
            source_batch_id string,
            source_batch_at timestamp,
            source_row_hash string,
            source_row_occurrence int,
            order_id string,
            customer_id string,
            product_id string,
            quantity int,
            unit_price_cents bigint,
            total_cents bigint,
            event_at timestamp,
            ingested_at timestamp,
            is_late boolean,
            quality_errors array<string>,
            reject_id string
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/silver/commerce_order_rejects'
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
    bronze = spark.table("lakehouse.bronze.commerce_orders").filter(
        F.col("batch_id") == batch_id
    )
    classified = classify(spark, bronze, batch_id=batch_id).cache()
    bronze_rows = classified.count()
    if bronze_rows == 0:
        raise RuntimeError(f"selected commerce order batch is empty: {batch_id}")

    valid = (
        classified.filter(F.size("quality_errors") == 0)
        .select(
            F.col("batch_id").alias("source_batch_id"),
            "source_batch_at",
            "source_row_hash",
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price_cents",
            "total_cents",
            "event_at",
            "ingested_at",
            "is_late",
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
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price_cents",
            "total_cents",
            "event_at",
            "ingested_at",
            "is_late",
            "quality_errors",
            "reject_id",
        )
        .cache()
    )
    valid_rows = valid.count()
    rejected_rows = rejects.count()
    if valid_rows + rejected_rows != bronze_rows:
        raise RuntimeError("commerce order quality counts do not reconcile")

    silver_before = spark.table("lakehouse.silver.commerce_orders").filter(
        F.col("source_batch_id") == batch_id
    ).count()
    rejects_before = spark.table(
        "lakehouse.silver.commerce_order_rejects"
    ).filter(F.col("source_batch_id") == batch_id).count()
    valid.createOrReplaceTempView("commerce_order_valid_source")
    spark.sql(
        """
        MERGE INTO lakehouse.silver.commerce_orders AS target
        USING commerce_order_valid_source AS source
        ON target.source_batch_id = source.source_batch_id
           AND target.order_id = source.order_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    rejects.createOrReplaceTempView("commerce_order_reject_source")
    spark.sql(
        """
        MERGE INTO lakehouse.silver.commerce_order_rejects AS target
        USING commerce_order_reject_source AS source
        ON target.reject_id = source.reject_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    silver_batch_rows = spark.table("lakehouse.silver.commerce_orders").filter(
        F.col("source_batch_id") == batch_id
    ).count()
    reject_batch_rows = spark.table(
        "lakehouse.silver.commerce_order_rejects"
    ).filter(F.col("source_batch_id") == batch_id).count()
    late_rows = spark.table("lakehouse.silver.commerce_orders").filter(
        (F.col("source_batch_id") == batch_id) & F.col("is_late")
    ).count()
    classified.unpersist()
    valid.unpersist()
    rejects.unpersist()
    if (silver_batch_rows, reject_batch_rows) != (valid_rows, rejected_rows):
        raise RuntimeError("commerce order silver post-condition failed")
    return {
        "status": "ready",
        "batch_id": batch_id,
        "bronze_rows": bronze_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "late_rows": late_rows,
        "silver_rows_before": silver_before,
        "silver_batch_rows": silver_batch_rows,
        "silver_rows_inserted": silver_batch_rows - silver_before,
        "reject_rows_before": rejects_before,
        "reject_batch_rows": reject_batch_rows,
        "reject_rows_inserted": reject_batch_rows - rejects_before,
    }


def main() -> None:
    batch_id = required_environment("COMMERCE_BATCH_ID")
    spark = build_session("lakehouse-ops-silver-commerce-orders")
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(write_selected_batch(spark, batch_id=batch_id), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
