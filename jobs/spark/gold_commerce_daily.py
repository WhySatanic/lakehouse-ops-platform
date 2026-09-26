from __future__ import annotations

import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from spark_catalog import build_session, required_environment

TABLE = "lakehouse.gold.commerce_daily"


def create_table(spark: SparkSession, bucket: str) -> None:
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold "
        f"LOCATION 's3a://{bucket}/warehouse/gold'"
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            source_batch_id string,
            order_day date,
            order_count bigint,
            buyer_count bigint,
            ordered_amount_cents bigint,
            captured_revenue_cents bigint,
            captured_payment_count bigint,
            noncaptured_payment_count bigint,
            rejected_payment_count bigint,
            missing_payment_count bigint,
            late_order_count bigint
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/gold/commerce_daily'
        PARTITIONED BY (source_batch_id)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )


def summarize(
    orders: DataFrame, payments: DataFrame, payment_rejects: DataFrame
) -> DataFrame:
    valid_by_order = payments.groupBy("order_id").agg(
        F.count("payment_id").alias("valid_payment_count"),
        F.sum(F.when(F.col("status") == "captured", 1).otherwise(0)).alias(
            "captured_payment_count"
        ),
        F.sum(F.when(F.col("status") != "captured", 1).otherwise(0)).alias(
            "noncaptured_payment_count"
        ),
        F.sum(
            F.when(F.col("status") == "captured", F.col("amount_cents")).otherwise(0)
        ).alias("captured_revenue_cents"),
    )
    rejected_by_order = payment_rejects.groupBy("order_id").agg(
        F.count("reject_id").alias("rejected_payment_count")
    )
    enriched = (
        orders.join(valid_by_order, "order_id", "left")
        .join(rejected_by_order, "order_id", "left")
        .fillna(
            0,
            subset=[
                "valid_payment_count",
                "captured_payment_count",
                "noncaptured_payment_count",
                "captured_revenue_cents",
                "rejected_payment_count",
            ],
        )
        .withColumn(
            "missing_payment_count",
            F.when(
                (F.col("valid_payment_count") + F.col("rejected_payment_count")) == 0,
                1,
            ).otherwise(0),
        )
        .withColumn("order_day", F.to_date("event_at"))
    )
    return enriched.groupBy("source_batch_id", "order_day").agg(
        F.count("order_id").alias("order_count"),
        F.countDistinct("customer_id").alias("buyer_count"),
        F.sum("total_cents").alias("ordered_amount_cents"),
        F.sum("captured_revenue_cents").alias("captured_revenue_cents"),
        F.sum("captured_payment_count").alias("captured_payment_count"),
        F.sum("noncaptured_payment_count").alias("noncaptured_payment_count"),
        F.sum("rejected_payment_count").alias("rejected_payment_count"),
        F.sum("missing_payment_count").alias("missing_payment_count"),
        F.sum(F.col("is_late").cast("bigint")).alias("late_order_count"),
    )


def validate_sources(spark: SparkSession, batch_id: str) -> tuple[DataFrame, DataFrame, DataFrame]:
    orders = spark.table("lakehouse.silver.commerce_orders").filter(
        F.col("source_batch_id") == batch_id
    )
    order_rejects = spark.table("lakehouse.silver.commerce_order_rejects").filter(
        F.col("source_batch_id") == batch_id
    )
    payments = spark.table("lakehouse.silver.commerce_payments").filter(
        F.col("source_batch_id") == batch_id
    )
    payment_rejects = spark.table("lakehouse.silver.commerce_payment_rejects").filter(
        F.col("source_batch_id") == batch_id
    )
    bronze_orders = spark.table("lakehouse.bronze.commerce_orders").filter(
        F.col("batch_id") == batch_id
    )
    bronze_payments = spark.table("lakehouse.bronze.commerce_payments").filter(
        F.col("batch_id") == batch_id
    )
    order_count = orders.count()
    payment_count = payments.count()
    if not order_count or not bronze_payments.count():
        raise RuntimeError(f"selected commerce batch is empty or incomplete: {batch_id}")
    if order_count + order_rejects.count() != bronze_orders.count():
        raise RuntimeError("commerce order silver is not reconciled to bronze")
    if payment_count + payment_rejects.count() != bronze_payments.count():
        raise RuntimeError("commerce payment silver is not reconciled to bronze")
    if orders.groupBy("order_id").count().filter(F.col("count") > 1).count():
        raise RuntimeError("commerce order silver contains duplicate order IDs")
    known_orders = orders.select("order_id")
    all_payments = payments.select("order_id").unionByName(
        payment_rejects.select("order_id")
    )
    if all_payments.join(known_orders, "order_id", "left_anti").count():
        raise RuntimeError("commerce payments contain orphaned or missing order IDs")
    for column, table in (
        ("customer_id", "lakehouse.silver.commerce_customers"),
        ("product_id", "lakehouse.silver.commerce_products"),
    ):
        known = spark.table(table).filter(F.col("source_batch_id") == batch_id).select(column)
        if orders.select(column).join(known, column, "left_anti").count():
            raise RuntimeError(f"commerce orders reference unvalidated {column}")
    return orders, payments, payment_rejects


def write_selected_batch(spark: SparkSession, *, batch_id: str) -> dict[str, int | str]:
    orders, payments, payment_rejects = validate_sources(spark, batch_id)
    daily = summarize(orders, payments, payment_rejects).cache()
    daily_rows = daily.count()
    if not daily_rows or daily.filter(F.col("order_day").isNull()).count():
        raise RuntimeError("commerce daily mart has no valid order days")
    create_table(spark, required_environment("LAKEHOUSE_BUCKET"))
    target_before = spark.table(TABLE).filter(F.col("source_batch_id") == batch_id)
    rows_before = target_before.count()
    if target_before.exceptAll(daily).count():
        raise RuntimeError("existing commerce daily rows differ from selected silver batch")
    daily.createOrReplaceTempView("commerce_daily_source")
    spark.sql(
        f"""
        MERGE INTO {TABLE} AS target
        USING commerce_daily_source AS source
        ON target.source_batch_id = source.source_batch_id
           AND target.order_day = source.order_day
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    target_after = spark.table(TABLE).filter(F.col("source_batch_id") == batch_id)
    rows_after = target_after.count()
    if target_after.exceptAll(daily).count() or daily.exceptAll(target_after).count():
        raise RuntimeError("commerce daily mart post-condition failed")
    metrics = daily.agg(
        F.sum("order_count").alias("orders"),
        F.sum("captured_revenue_cents").alias("captured_revenue_cents"),
        F.sum("rejected_payment_count").alias("rejected_payments"),
    ).first()
    daily.unpersist()
    return {
        "status": "ready",
        "batch_id": batch_id,
        "days": daily_rows,
        "orders": metrics.orders,
        "captured_revenue_cents": metrics.captured_revenue_cents,
        "rejected_payments": metrics.rejected_payments,
        "rows_inserted": rows_after - rows_before,
    }


def main() -> None:
    batch_id = required_environment("COMMERCE_BATCH_ID")
    spark = build_session("lakehouse-ops-gold-commerce-daily")
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(write_selected_batch(spark, batch_id=batch_id), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
