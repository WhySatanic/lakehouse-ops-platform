from __future__ import annotations

import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"required environment variable is not set: {name}")
    return value


def source_schema() -> T.StructType:
    hourly = T.StructType(
        [
            T.StructField("time", T.ArrayType(T.StringType()), nullable=False),
            T.StructField("temperature_2m", T.ArrayType(T.DoubleType()), nullable=False),
            T.StructField("relative_humidity_2m", T.ArrayType(T.DoubleType()), nullable=False),
            T.StructField("precipitation", T.ArrayType(T.DoubleType()), nullable=False),
            T.StructField("wind_speed_10m", T.ArrayType(T.DoubleType()), nullable=False),
        ]
    )
    return T.StructType(
        [
            T.StructField(
                "ingestion",
                T.StructType(
                    [
                        T.StructField("source", T.StringType(), nullable=False),
                        T.StructField("ingested_at", T.StringType(), nullable=False),
                        T.StructField(
                            "location",
                            T.StructType(
                                [
                                    T.StructField("name", T.StringType(), nullable=False),
                                    T.StructField("latitude", T.DoubleType(), nullable=False),
                                    T.StructField("longitude", T.DoubleType(), nullable=False),
                                ]
                            ),
                            nullable=False,
                        ),
                        T.StructField("object_checksum", T.StringType(), nullable=False),
                    ]
                ),
                nullable=False,
            ),
            T.StructField(
                "payload",
                T.StructType([T.StructField("hourly", hourly, nullable=False)]),
                nullable=False,
            ),
        ]
    )


def validate_source(frame: DataFrame) -> int:
    source_count = frame.count()
    if source_count == 0:
        raise RuntimeError("landing input contains no JSON documents")

    hourly = F.col("payload.hourly")
    time_count = F.size(hourly.time)
    invalid = frame.filter(
        hourly.isNull()
        | (time_count <= 0)
        | (F.size(hourly.temperature_2m) != time_count)
        | (F.size(hourly.relative_humidity_2m) != time_count)
        | (F.size(hourly.precipitation) != time_count)
        | (F.size(hourly.wind_speed_10m) != time_count)
    ).count()
    if invalid:
        raise RuntimeError(f"landing input contains {invalid} malformed document(s)")
    return source_count


def transform_source(frame: DataFrame) -> DataFrame:
    expanded = frame.select(
        "ingestion",
        "payload.hourly",
        F.posexplode("payload.hourly.time").alias("position", "observed_at_raw"),
    )
    array_index = F.col("position") + F.lit(1)
    return expanded.select(
        F.col("ingestion.object_checksum").alias("object_checksum"),
        F.col("ingestion.source").alias("source"),
        F.to_timestamp("ingestion.ingested_at").alias("ingested_at"),
        F.col("ingestion.location.name").alias("location_name"),
        F.col("ingestion.location.latitude").alias("latitude"),
        F.col("ingestion.location.longitude").alias("longitude"),
        F.to_timestamp("observed_at_raw", "yyyy-MM-dd'T'HH:mm").alias("observed_at"),
        F.element_at("hourly.temperature_2m", array_index).alias("temperature_2m"),
        F.element_at("hourly.relative_humidity_2m", array_index).alias(
            "relative_humidity_2m"
        ),
        F.element_at("hourly.precipitation", array_index).alias("precipitation"),
        F.element_at("hourly.wind_speed_10m", array_index).alias("wind_speed_10m"),
    )


def build_session() -> SparkSession:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    endpoint = required_environment("LAKEOPS_S3_ENDPOINT_URL")
    access_key = required_environment("AWS_ACCESS_KEY_ID")
    secret_key = required_environment("AWS_SECRET_ACCESS_KEY")
    metastore_uri = required_environment("HIVE_METASTORE_URI")
    return (
        SparkSession.builder.appName("lakehouse-ops-bronze-weather")
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", metastore_uri)
        .config("spark.sql.catalog.lakehouse.warehouse", f"s3://{bucket}/warehouse")
        .config(
            "spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.sql.catalog.lakehouse.s3.endpoint", endpoint)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", access_key)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", secret_key)
        .config(
            "spark.sql.catalog.lakehouse.client.region",
            os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        )
        .getOrCreate()
    )


def write_bronze(spark: SparkSession, source: DataFrame) -> dict[str, int | str]:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.weather_hourly (
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
        PARTITIONED BY (days(observed_at), location_name)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )
    source.createOrReplaceTempView("bronze_weather_source")
    before = spark.table("lakehouse.bronze.weather_hourly").count()
    spark.sql(
        """
        MERGE INTO lakehouse.bronze.weather_hourly AS target
        USING bronze_weather_source AS source
        ON target.object_checksum = source.object_checksum
           AND target.observed_at = source.observed_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    after = spark.table("lakehouse.bronze.weather_hourly").count()
    snapshot_count = spark.sql(
        "SELECT count(*) AS count FROM lakehouse.bronze.weather_hourly.snapshots"
    ).first()["count"]
    return {
        "status": "ready",
        "table": "lakehouse.bronze.weather_hourly",
        "source_rows": source.count(),
        "rows_before": before,
        "rows_after": after,
        "rows_inserted": after - before,
        "snapshots": snapshot_count,
    }


def main() -> None:
    input_root = Path(os.environ.get("BRONZE_INPUT_ROOT", "/opt/lakehouse/input"))
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = (
            spark.read.schema(source_schema())
            .option("multiLine", "true")
            .option("recursiveFileLookup", "true")
            .json(str(input_root))
        )
        source_documents = validate_source(raw)
        transformed = transform_source(raw)
        report = write_bronze(spark, transformed)
        report["source_documents"] = source_documents
        print(json.dumps(report, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
