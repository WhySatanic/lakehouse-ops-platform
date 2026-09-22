from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from spark_catalog import build_session, required_environment

from lakehouse_ops.ingestion.commerce_bronze import (
    CommerceBatchDirectory,
    CommerceTableFile,
    load_commerce_batch,
)


@dataclass(frozen=True, slots=True)
class TableContract:
    schema: T.StructType
    ddl: str
    required: tuple[str, ...]


TABLES = {
    "customers": TableContract(
        T.StructType(
            [
                T.StructField("customer_id", T.StringType(), False),
                T.StructField("full_name", T.StringType(), False),
                T.StructField("email", T.StringType(), True),
                T.StructField("registered_at", T.TimestampType(), False),
            ]
        ),
        "customer_id string, full_name string, email string, registered_at timestamp",
        ("customer_id", "full_name", "registered_at"),
    ),
    "orders": TableContract(
        T.StructType(
            [
                T.StructField("order_id", T.StringType(), False),
                T.StructField("customer_id", T.StringType(), False),
                T.StructField("product_id", T.StringType(), False),
                T.StructField("quantity", T.IntegerType(), False),
                T.StructField("unit_price_cents", T.LongType(), False),
                T.StructField("total_cents", T.LongType(), False),
                T.StructField("event_at", T.TimestampType(), False),
                T.StructField("ingested_at", T.TimestampType(), False),
            ]
        ),
        (
            "order_id string, customer_id string, product_id string, quantity int, "
            "unit_price_cents bigint, total_cents bigint, event_at timestamp, "
            "ingested_at timestamp"
        ),
        (
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price_cents",
            "total_cents",
            "event_at",
            "ingested_at",
        ),
    ),
    "payments": TableContract(
        T.StructType(
            [
                T.StructField("payment_id", T.StringType(), False),
                T.StructField("order_id", T.StringType(), False),
                T.StructField("amount_cents", T.LongType(), True),
                T.StructField("status", T.StringType(), False),
                T.StructField("paid_at", T.TimestampType(), False),
            ]
        ),
        (
            "payment_id string, order_id string, amount_cents bigint, status string, "
            "paid_at timestamp"
        ),
        ("payment_id", "order_id", "status", "paid_at"),
    ),
    "products": TableContract(
        T.StructType(
            [
                T.StructField("product_id", T.StringType(), False),
                T.StructField("name", T.StringType(), False),
                T.StructField("category", T.StringType(), False),
                T.StructField("unit_price_cents", T.LongType(), False),
            ]
        ),
        "product_id string, name string, category string, unit_price_cents bigint",
        ("product_id", "name", "category", "unit_price_cents"),
    ),
}


def prepare_source(
    spark: SparkSession,
    batch: CommerceBatchDirectory,
    table_file: CommerceTableFile,
) -> DataFrame:
    contract = TABLES[table_file.name]
    source = (
        spark.read.schema(contract.schema)
        .option("mode", "FAILFAST")
        .json(str(table_file.path))
    )
    source_rows = source.count()
    if source_rows != table_file.rows:
        raise RuntimeError(
            f"Spark row count mismatch for {table_file.name}: "
            f"expected {table_file.rows}, observed {source_rows}"
        )
    missing = F.lit(False)
    for column in contract.required:
        missing = missing | F.col(column).isNull()
    invalid_rows = source.filter(missing).count()
    if invalid_rows:
        raise RuntimeError(
            f"required values are missing in {invalid_rows} {table_file.name} row(s)"
        )

    data_columns = source.columns
    with_hash = source.withColumn(
        "source_row_hash",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(column) for column in data_columns]),
                options={"ignoreNullFields": "false"},
            ),
            256,
        ),
    )
    occurrence = Window.partitionBy("source_row_hash").orderBy(F.monotonically_increasing_id())
    return (
        with_hash.withColumn("source_row_occurrence", F.row_number().over(occurrence))
        .withColumn("batch_id", F.lit(batch.batch_id))
        .withColumn("source_batch_at", F.to_timestamp(F.lit(batch.batch_at)))
        .withColumn("source_file_sha256", F.lit(table_file.sha256))
        .select(
            "batch_id",
            "source_batch_at",
            "source_file_sha256",
            "source_row_hash",
            "source_row_occurrence",
            *data_columns,
        )
    )


def create_table(
    spark: SparkSession, *, bucket: str, name: str, contract: TableContract
) -> str:
    table = f"lakehouse.bronze.commerce_{name}"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            batch_id string,
            source_batch_at timestamp,
            source_file_sha256 string,
            source_row_hash string,
            source_row_occurrence int,
            {contract.ddl}
        )
        USING iceberg
        LOCATION 's3a://{bucket}/warehouse/bronze/commerce_{name}'
        PARTITIONED BY (batch_id)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )
    return table


def merge_table(
    spark: SparkSession,
    *,
    batch: CommerceBatchDirectory,
    table_file: CommerceTableFile,
    bucket: str,
) -> dict[str, int | str]:
    table = create_table(
        spark, bucket=bucket, name=table_file.name, contract=TABLES[table_file.name]
    )
    source = prepare_source(spark, batch, table_file).cache()
    view = f"commerce_{table_file.name}_source"
    source.createOrReplaceTempView(view)
    rows_before = spark.table(table).count()
    spark.sql(
        f"""
        MERGE INTO {table} AS target
        USING {view} AS source
        ON target.batch_id = source.batch_id
           AND target.source_row_hash = source.source_row_hash
           AND target.source_row_occurrence = source.source_row_occurrence
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    rows_after = spark.table(table).count()
    batch_rows = spark.table(table).filter(F.col("batch_id") == batch.batch_id).count()
    source.unpersist()
    if batch_rows != table_file.rows:
        raise RuntimeError(
            f"bronze post-condition failed for {table_file.name}: "
            f"expected {table_file.rows}, observed {batch_rows}"
        )
    return {
        "source_rows": table_file.rows,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "rows_inserted": rows_after - rows_before,
        "table": table,
    }


def write_batch(spark: SparkSession, batch: CommerceBatchDirectory) -> dict[str, object]:
    bucket = required_environment("LAKEHOUSE_BUCKET")
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze "
        f"LOCATION 's3a://{bucket}/warehouse/bronze'"
    )
    tables = {
        table_file.name: merge_table(
            spark, batch=batch, table_file=table_file, bucket=bucket
        )
        for table_file in batch.tables
    }
    return {"status": "ready", "batch_id": batch.batch_id, "tables": tables}


def main() -> None:
    batch_id = required_environment("COMMERCE_BATCH_ID")
    input_root = Path(os.environ.get("COMMERCE_INPUT_ROOT", "/opt/lakehouse/input"))
    batch_path = input_root / "source=commerce" / f"batch_id={batch_id}"
    batch = load_commerce_batch(batch_path, expected_batch_id=batch_id)
    spark = build_session("lakehouse-ops-bronze-commerce")
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(write_batch(spark, batch), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
