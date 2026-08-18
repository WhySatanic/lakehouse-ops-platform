from __future__ import annotations

import json

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from spark_catalog import build_session, required_environment


def quality_errors() -> Column:
    candidates = F.array(
        F.when(
            F.col("object_checksum").isNull()
            | (F.length(F.trim(F.col("object_checksum"))) == 0),
            F.lit("missing_object_checksum"),
        ),
        F.when(
            F.col("location_name").isNull()
            | (F.length(F.trim(F.col("location_name"))) == 0),
            F.lit("missing_location_name"),
        ),
        F.when(
            F.col("latitude").isNull() | ~F.col("latitude").between(-90.0, 90.0),
            F.lit("latitude_out_of_range"),
        ),
        F.when(
            F.col("longitude").isNull() | ~F.col("longitude").between(-180.0, 180.0),
            F.lit("longitude_out_of_range"),
        ),
        F.when(F.col("ingested_at").isNull(), F.lit("missing_ingested_at")),
        F.when(F.col("observed_at").isNull(), F.lit("missing_observed_at")),
        F.when(
            F.col("temperature_2m").isNull()
            | ~F.col("temperature_2m").between(-100.0, 70.0),
            F.lit("temperature_out_of_range"),
        ),
        F.when(
            F.col("relative_humidity_2m").isNull()
            | ~F.col("relative_humidity_2m").between(0.0, 100.0),
            F.lit("humidity_out_of_range"),
        ),
        F.when(
            F.col("precipitation").isNull() | (F.col("precipitation") < 0.0),
            F.lit("negative_precipitation"),
        ),
        F.when(
            F.col("wind_speed_10m").isNull() | (F.col("wind_speed_10m") < 0.0),
            F.lit("negative_wind_speed"),
        ),
    )
    return F.filter(candidates, lambda error: error.isNotNull())


def classify(source: DataFrame) -> DataFrame:
    return source.withColumn("quality_errors", quality_errors())


def select_survivors(valid: DataFrame) -> DataFrame:
    survivor_order = Window.partitionBy("location_name", "observed_at").orderBy(
        F.col("ingested_at").desc(), F.col("object_checksum").desc()
    )
    return (
        valid.withColumn("_survivor_rank", F.row_number().over(survivor_order))
        .filter(F.col("_survivor_rank") == 1)
        .drop("_survivor_rank", "quality_errors")
    )


def select_rejects(classified: DataFrame) -> DataFrame:
    source_columns = [column for column in classified.columns if column != "quality_errors"]
    return (
        classified.filter(F.size("quality_errors") > 0)
        .withColumn(
            "reject_id",
            F.sha2(F.to_json(F.struct(*source_columns)), 256),
        )
        .dropDuplicates(["reject_id"])
    )


def create_tables(spark: SparkSession) -> None:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver "
        f"LOCATION 's3a://{bucket}/warehouse/silver'"
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.weather_hourly (
            object_checksum string,
            source string,
            ingested_at timestamp,
            location_name string,
            latitude double,
            longitude double,
            observed_at timestamp,
            temperature_2m double,
            relative_humidity_2m double,
            precipitation double,
            wind_speed_10m double
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/silver/weather_hourly'
        PARTITIONED BY (days(observed_at), location_name)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.weather_hourly_rejects (
            object_checksum string,
            source string,
            ingested_at timestamp,
            location_name string,
            latitude double,
            longitude double,
            observed_at timestamp,
            temperature_2m double,
            relative_humidity_2m double,
            precipitation double,
            wind_speed_10m double,
            quality_errors array<string>,
            reject_id string
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/silver/weather_hourly_rejects'
        PARTITIONED BY (days(ingested_at), location_name)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )


def write_silver(spark: SparkSession) -> dict[str, int | str]:
    create_tables(spark)
    bronze = spark.table("lakehouse.bronze.weather_hourly")
    classified = classify(bronze).cache()
    valid = classified.filter(F.size("quality_errors") == 0)
    rejected = select_rejects(classified).cache()
    survivors = select_survivors(valid).cache()

    bronze_rows = classified.count()
    valid_rows = valid.count()
    rejected_rows = rejected.count()
    survivor_rows = survivors.count()
    silver_before = spark.table("lakehouse.silver.weather_hourly").count()
    rejects_before = spark.table("lakehouse.silver.weather_hourly_rejects").count()

    survivors.createOrReplaceTempView("silver_weather_source")
    spark.sql(
        """
        MERGE INTO lakehouse.silver.weather_hourly AS target
        USING silver_weather_source AS source
        ON target.location_name = source.location_name
           AND target.observed_at = source.observed_at
        WHEN MATCHED AND target.object_checksum <> source.object_checksum
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    rejected.createOrReplaceTempView("silver_weather_reject_source")
    spark.sql(
        """
        MERGE INTO lakehouse.silver.weather_hourly_rejects AS target
        USING silver_weather_reject_source AS source
        ON target.reject_id = source.reject_id
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    silver_after = spark.table("lakehouse.silver.weather_hourly").count()
    rejects_after = spark.table("lakehouse.silver.weather_hourly_rejects").count()
    classified.unpersist()
    survivors.unpersist()
    rejected.unpersist()
    return {
        "status": "ready",
        "table": "lakehouse.silver.weather_hourly",
        "reject_table": "lakehouse.silver.weather_hourly_rejects",
        "bronze_rows": bronze_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "duplicate_rows": valid_rows - survivor_rows,
        "silver_rows_before": silver_before,
        "silver_rows_after": silver_after,
        "reject_rows_before": rejects_before,
        "reject_rows_after": rejects_after,
    }


def main() -> None:
    spark = build_session("lakehouse-ops-silver-weather")
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(write_silver(spark), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
