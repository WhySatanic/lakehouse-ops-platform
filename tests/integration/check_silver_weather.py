from __future__ import annotations

import json
import os

from pyspark.sql import functions as F
from spark_catalog import build_session


def expected_integer(name: str) -> int:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"required environment variable is not set: {name}")
    return int(value)


def main() -> None:
    spark = build_session("lakehouse-ops-silver-contract-check")
    spark.sparkContext.setLogLevel("WARN")
    try:
        silver = spark.table("lakehouse.silver.weather_hourly")
        rejects = spark.table("lakehouse.silver.weather_hourly_rejects")
        silver_rows = silver.count()
        reject_rows = rejects.count()
        assert silver_rows == expected_integer("EXPECTED_SILVER_ROWS")
        assert reject_rows == expected_integer("EXPECTED_REJECT_ROWS")

        duplicate_keys = (
            silver.groupBy("location_name", "observed_at")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        assert duplicate_keys == 0

        expected_checksum = os.environ["EXPECTED_WINNER_CHECKSUM"]
        winner = (
            silver.filter(
                (F.col("location_name") == "moscow")
                & (F.col("observed_at") == F.to_timestamp(F.lit("2026-08-06 00:00:00")))
            )
            .select("object_checksum", "temperature_2m")
            .first()
        )
        assert winner is not None
        assert winner["object_checksum"] == expected_checksum
        assert winner["temperature_2m"] == 19.0

        humidity_rejects = rejects.filter(
            F.array_contains("quality_errors", "humidity_out_of_range")
        ).count()
        assert humidity_rejects == 1
        print(
            json.dumps(
                {
                    "status": "ready",
                    "silver_rows": silver_rows,
                    "reject_rows": reject_rows,
                    "duplicate_keys": duplicate_keys,
                    "humidity_rejects": humidity_rejects,
                    "winner_checksum": winner["object_checksum"],
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
