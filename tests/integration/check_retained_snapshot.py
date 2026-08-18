from __future__ import annotations

import json

from spark_catalog import build_session, required_environment


def main() -> None:
    report_path = required_environment("SNAPSHOT_EXPIRATION_REPORT_PATH")
    with open(report_path, encoding="utf-8") as source:
        report = json.load(source)
    table = report["table"]
    before_ids = set(report["before"]["snapshot_ids"])
    after_ids = set(report["after"]["snapshot_ids"])
    current_id = report["after"]["snapshot_id"]
    retained_id = next(iter(after_ids - {current_id}))
    expired_id = next(iter(before_ids - after_ids))

    spark = build_session("lakehouse-ops-snapshot-retention-check")
    spark.sparkContext.setLogLevel("WARN")
    try:
        current_rows = spark.table(table).count()
        retained_rows = (
            spark.read.format("iceberg")
            .option("snapshot-id", retained_id)
            .load(table)
            .count()
        )
        try:
            spark.read.format("iceberg").option("snapshot-id", expired_id).load(table).count()
        except Exception as error:
            message = str(error).lower()
            expired_unavailable = "snapshot" in message and any(
                marker in message
                for marker in ("not found", "does not exist", "cannot find")
            )
        else:
            expired_unavailable = False
        if current_rows != 4 or retained_rows != 4 or not expired_unavailable:
            raise RuntimeError(
                "snapshot retention check failed: "
                f"current={current_rows}, retained={retained_rows}, "
                f"expired_unavailable={expired_unavailable}"
            )
        print(
            json.dumps(
                {
                    "status": "ready",
                    "current_rows": current_rows,
                    "retained_snapshot_id": retained_id,
                    "retained_rows": retained_rows,
                    "expired_snapshot_id": expired_id,
                    "expired_unavailable": expired_unavailable,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
