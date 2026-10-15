"""Real Spark smoke for the production session hook, not full commerce throughput."""

from check_commerce_bronze_validation import check as check_bronze_validation
from spark_catalog import build_session

spark = build_session("spark-container-memory-smoke")
try:
    spark.sparkContext.setLogLevel("WARN")
    spark.range(100_000).repartition(2).write.mode("overwrite").parquet("/tmp/memory-smoke")
    assert spark.read.parquet("/tmp/memory-smoke").count() == 100_000
    check_bronze_validation(spark)
finally:
    spark.stop()
