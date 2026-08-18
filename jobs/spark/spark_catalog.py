from __future__ import annotations

import os

from pyspark.sql import SparkSession


def required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"required environment variable is not set: {name}")
    return value


def build_session(app_name: str) -> SparkSession:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    endpoint = required_environment("LAKEOPS_S3_ENDPOINT_URL")
    access_key = required_environment("AWS_ACCESS_KEY_ID")
    secret_key = required_environment("AWS_SECRET_ACCESS_KEY")
    metastore_uri = required_environment("HIVE_METASTORE_URI")
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", metastore_uri)
        .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{bucket}/warehouse")
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
