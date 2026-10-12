"""Exercise forward SCD2 publication boundaries in the isolated acceptance catalog."""

from unittest.mock import patch

from commerce_customer_scd2 import TABLE, apply_customer_history, validate_target
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def check_atomic_history(spark: SparkSession, customers: DataFrame) -> None:
    batch_id = "a" * 16
    source = (
        customers.withColumn("full_name", F.lit("Alice Final"))
        .withColumn("source_batch_id", F.lit(batch_id))
        .withColumn("source_batch_at", F.to_timestamp(F.lit("2026-02-05T00:00:00Z")))
    )
    newcomer = source.withColumn("customer_id", F.lit("C00000002"))
    source = source.unionByName(newcomer)
    before = spark.table(TABLE).orderBy("customer_version_id").collect()
    snapshots = spark.table(f"{TABLE}.snapshots").count()
    original_sql = spark.sql

    def fail_before(sql, *args, **kwargs):
        if sql.lstrip().startswith("MERGE INTO"):
            raise RuntimeError("injected before publication")
        return original_sql(sql, *args, **kwargs)

    with patch.object(spark, "sql", side_effect=fail_before):
        try:
            apply_customer_history(spark, source, batch_id=batch_id)
        except RuntimeError as error:
            assert str(error) == "injected before publication"
        else:
            raise AssertionError("pre-publication failure was not injected")
    assert spark.table(TABLE).orderBy("customer_version_id").collect() == before
    assert spark.table(f"{TABLE}.snapshots").count() == snapshots

    def fail_after(sql, *args, **kwargs):
        result = original_sql(sql, *args, **kwargs)
        if sql.lstrip().startswith("MERGE INTO"):
            published = spark.table(TABLE)
            current = published.filter("is_current").orderBy("customer_id").collect()
            assert [(row.customer_id, row.full_name) for row in current] == [
                ("C00000001", "Alice Final"), ("C00000002", "Alice Final"),
            ], "publication exposed closed history without its replacement"
            assert published.count() == len(before) + 2
            validate_target(spark)
            raise RuntimeError("injected after publication")
        return result

    with patch.object(spark, "sql", side_effect=fail_after):
        try:
            apply_customer_history(spark, source, batch_id=batch_id)
        except RuntimeError as error:
            assert str(error) == "injected after publication"
        else:
            raise AssertionError("post-publication failure was not injected")
    after = spark.table(TABLE).orderBy("customer_version_id").collect()
    assert spark.table(f"{TABLE}.snapshots").count() == snapshots + 1
    assert apply_customer_history(spark, source, batch_id=batch_id)["rows_inserted"] == 0
    assert spark.table(TABLE).orderBy("customer_version_id").collect() == after
    assert spark.table(f"{TABLE}.snapshots").count() == snapshots + 1
    # Corrupt only this isolated test table to verify the missing-current invariant.
    original_sql(f"UPDATE {TABLE} SET is_current=false, "
                 "valid_to=TIMESTAMP '2026-02-06 00:00:00' WHERE customer_id='C00000002'")
    try:
        validate_target(spark)
    except RuntimeError as error:
        assert "invariants failed" in str(error)
    else:
        raise AssertionError("customer without a current version was accepted")
