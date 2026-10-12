from __future__ import annotations

import json
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from spark_catalog import build_session, required_environment

TABLE = "lakehouse.gold.dim_customers_scd2"


def rebuild_customer_history(spark: SparkSession, source: DataFrame) -> dict[str, int | str]:
    """Atomically rebuild observed-state history from complete validated silver input.

    Opt-in, single-writer operation. Effective time remains source_batch_at, not
    arrival order; absence from a full snapshot is not a customer deletion event.
    """
    source = with_history_columns(source).cache()
    history = None
    try:
        source_rows = source.count()
        if not source_rows:
            raise ValueError("history rebuild requires nonempty complete silver history")
        required = ["customer_id", "full_name", "registered_at", "source_batch_id",
                    "source_batch_at", "email_is_missing"]
        invalid = F.lit(False)
        for column in required:
            invalid = invalid | F.col(column).isNull()
        if source.filter(invalid).count():
            raise ValueError("history rebuild source has missing required values")
        if source.groupBy("customer_id", "source_batch_at").count().filter("count > 1").count():
            raise ValueError("history rebuild has ambiguous customer effective timestamps")
        if source.groupBy("customer_id", "source_batch_id").count().filter("count > 1").count():
            raise ValueError("history rebuild has duplicate customer batch identities")
        ordered = Window.partitionBy("customer_id").orderBy("valid_from")
        changes = (
            source.withColumn("previous_hash", F.lag("attribute_hash").over(ordered))
            .filter(F.col("previous_hash").isNull()
                    | (F.col("previous_hash") != F.col("attribute_hash")))
        )
        history = changes.select(
            F.sha2(F.concat_ws(":", "customer_id", "attribute_hash", "source_batch_id"),
                   256).alias("customer_version_id"),
            "customer_id", "full_name", "email", "email_is_missing", "registered_at",
            "attribute_hash", "source_batch_id", "valid_from",
            F.lead("valid_from").over(ordered).alias("valid_to"),
        ).withColumn("is_current", F.col("valid_to").isNull()).cache()
        target = spark.table(TABLE).select(*history.columns)
        # Never erase customers or their earliest known version through truncated input.
        starts = source.groupBy("customer_id").agg(F.min("valid_from").alias("source_start"))
        coverage = target.join(starts, "customer_id", "left")
        if coverage.filter(F.col("source_start").isNull()
                           | (F.col("source_start") > F.col("valid_from"))).count():
            raise ValueError("history rebuild source does not cover existing history")
        rows = history.count()
        action = "replay"
        if history.exceptAll(target).count() or target.exceptAll(history).count():
            history.writeTo(TABLE).overwrite(F.lit(True))
            action = "replace"
        validate_target(spark)
        published = spark.table(TABLE).select(*history.columns)
        if history.exceptAll(published).count() or published.exceptAll(history).count():
            raise RuntimeError("history rebuild publication post-condition failed")
        return {"status": "ready", "mode": "rebuild", "action": action,
                "source_rows": source_rows, "history_rows": rows}
    finally:
        source.unpersist()
        if history is not None:
            history.unpersist()


def with_history_columns(source: DataFrame) -> DataFrame:
    return source.withColumn(
        "attribute_hash",
        F.sha2(
            F.to_json(
                F.struct("full_name", "email", "email_is_missing", "registered_at"),
                {"ignoreNullFields": "false"},
            ),
            256,
        ),
    ).withColumn("valid_from", F.col("source_batch_at"))


def create_table(spark: SparkSession, bucket: str) -> None:
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold "
        f"LOCATION 's3a://{bucket}/warehouse/gold'"
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_customers_scd2 (
            customer_version_id string,
            customer_id string,
            full_name string,
            email string,
            email_is_missing boolean,
            registered_at timestamp,
            attribute_hash string,
            source_batch_id string,
            valid_from timestamp,
            valid_to timestamp,
            is_current boolean
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/gold/dim_customers_scd2'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )


def validate_target(spark: SparkSession) -> None:
    target = spark.table("lakehouse.gold.dim_customers_scd2")
    duplicate_current = (
        target.filter(F.col("is_current"))
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    invalid_periods = target.filter(
        (F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from")))
        | (F.col("is_current") & F.col("valid_to").isNotNull())
        | (~F.col("is_current") & F.col("valid_to").isNull())
    ).count()
    ordered = Window.partitionBy("customer_id").orderBy("valid_from")
    discontinuous_periods = (
        target.withColumn("previous_valid_to", F.lag("valid_to").over(ordered))
        .filter(
            F.col("previous_valid_to").isNotNull()
            & (F.col("previous_valid_to") != F.col("valid_from"))
        )
        .count()
    )
    if duplicate_current or invalid_periods or discontinuous_periods:
        raise RuntimeError("customer SCD2 target invariants failed")


def write_selected_batch(
    spark: SparkSession, *, batch_id: str
) -> dict[str, int | str]:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    create_table(spark, bucket)
    return apply_customer_history(
        spark,
        spark.table("lakehouse.silver.commerce_customers").filter(
            F.col("source_batch_id") == batch_id
        ),
        batch_id=batch_id,
    )


def apply_customer_history(
    spark: SparkSession, source: DataFrame, *, batch_id: str,
) -> dict[str, int | str]:
    """Apply selected validated silver rows to an existing Iceberg history table."""
    source = with_history_columns(source).cache()
    source_rows = source.count()
    if source_rows == 0:
        raise RuntimeError(f"selected commerce customer silver batch is empty: {batch_id}")
    if source.select("source_batch_at").distinct().count() != 1:
        raise RuntimeError("selected customer batch has multiple effective timestamps")
    if source.select("customer_id").distinct().count() != source_rows:
        raise RuntimeError("selected customer batch contains duplicate customer IDs")

    validate_target(spark)
    target = spark.table("lakehouse.gold.dim_customers_scd2")
    current = target.filter(F.col("is_current")).select(
        "customer_id", "attribute_hash", "valid_from"
    )
    out_of_order = (
        source.alias("source")
        .join(current.alias("current"), "customer_id")
        .filter(
            (F.col("source.attribute_hash") != F.col("current.attribute_hash"))
            & (F.col("source.valid_from") <= F.col("current.valid_from"))
        )
        .count()
    )
    if out_of_order:
        raise RuntimeError("selected customer batch is not newer than current history")

    changed = (
        source.alias("source")
        .join(current.alias("current"), "customer_id", "left")
        .filter(
            F.col("current.customer_id").isNull()
            | (F.col("source.attribute_hash") != F.col("current.attribute_hash"))
        )
        .select("source.*")
        .cache()
    )
    changed_rows = changed.count()
    target_rows_before = target.count()
    current_rows_before = target.filter(F.col("is_current")).count()

    changed.createOrReplaceTempView("commerce_customer_history_changes")
    spark.sql(
        """
        MERGE INTO lakehouse.gold.dim_customers_scd2 AS target
        USING commerce_customer_history_changes AS source
        ON target.customer_id = source.customer_id
           AND target.is_current = true
        WHEN MATCHED AND target.attribute_hash <> source.attribute_hash THEN
          UPDATE SET valid_to = source.valid_from, is_current = false
        """
    )
    new_versions = changed.select(
        F.sha2(
            F.concat_ws(":", "customer_id", "attribute_hash", "source_batch_id"),
            256,
        ).alias("customer_version_id"),
        "customer_id",
        "full_name",
        "email",
        "email_is_missing",
        "registered_at",
        "attribute_hash",
        "source_batch_id",
        "valid_from",
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
    )
    if changed_rows:
        new_versions.writeTo("lakehouse.gold.dim_customers_scd2").append()

    validate_target(spark)
    target_after = spark.table("lakehouse.gold.dim_customers_scd2")
    target_rows_after = target_after.count()
    current_rows_after = target_after.filter(F.col("is_current")).count()
    selected_current = source.alias("source").join(
        target_after.filter(F.col("is_current")).alias("target"),
        "customer_id",
    )
    matched_current_rows = selected_current.count()
    mismatched_current = (
        selected_current
        .filter(F.col("source.attribute_hash") != F.col("target.attribute_hash"))
        .count()
    )
    source.unpersist()
    changed.unpersist()
    if (
        target_rows_after - target_rows_before != changed_rows
        or matched_current_rows != source_rows
        or mismatched_current
    ):
        raise RuntimeError("customer SCD2 post-condition failed")
    return {
        "status": "ready",
        "batch_id": batch_id,
        "source_rows": source_rows,
        "changed_rows": changed_rows,
        "target_rows_before": target_rows_before,
        "target_rows_after": target_rows_after,
        "rows_inserted": target_rows_after - target_rows_before,
        "current_rows_before": current_rows_before,
        "current_rows_after": current_rows_after,
    }


def main() -> None:
    mode = os.environ.get("COMMERCE_CUSTOMER_HISTORY_MODE", "forward")
    if mode not in {"forward", "rebuild"}:
        raise ValueError("COMMERCE_CUSTOMER_HISTORY_MODE must be forward or rebuild")
    batch_id = required_environment("COMMERCE_BATCH_ID") if mode == "forward" else None
    spark = build_session("lakehouse-ops-commerce-customer-scd2")
    spark.sparkContext.setLogLevel("WARN")
    try:
        if mode == "rebuild":
            create_table(spark, required_environment("LAKEHOUSE_BUCKET"))
            report = rebuild_customer_history(
                spark, spark.table("lakehouse.silver.commerce_customers"),
            )
        else:
            report = write_selected_batch(spark, batch_id=batch_id)
        print(json.dumps(report, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
