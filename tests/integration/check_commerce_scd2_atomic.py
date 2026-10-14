"""Exercise forward SCD2 publication boundaries in the isolated acceptance catalog."""

import hashlib
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import commerce_customer_scd2 as history
from commerce_customer_scd2 import TABLE, apply_customer_history, validate_target
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse_ops.ingestion.commerce_batches import CommerceBatchPlanner


def check_inflight_history(spark: SparkSession, customers: DataFrame) -> None:
    """Real history publication with the production queue and an in-memory landing marker."""
    table = TABLE
    schema = spark.table(table).schema
    # Prior assertions are complete; reset only this disposable acceptance table.
    spark.sql(f"DROP TABLE {table}")
    spark.createDataFrame([], schema).writeTo(table).using("iceberg").create()
    objects = {}
    sources = {}

    class Landing:
        def list_objects_v2(self, **kwargs):
            return {"Contents": [{"Key": key} for key in objects], "IsTruncated": False}

        def get_object(self, **kwargs):
            body, metadata = objects[kwargs["Key"]]
            return {"Body": io.BytesIO(body), "Metadata": metadata}

    def land(identity, day):
        body = json.dumps({"schema_version": 1, "batch_id": identity,
                           "config": {"batch_at": f"2026-03-{day:02d}T00:00:00Z",
                                      "source_semantics": "full_snapshot"},
                           "tables": {name: {"file": f"{name}.jsonl", "rows": 1,
                                             "sha256": "0" * 64}
                                      for name in ("customers", "products", "orders", "payments")}}
                          ).encode()
        objects[f"landing/source=commerce/batch_id={identity}/manifest.json"] = (
            body, {"commit-marker": "true", "source": "commerce", "batch-id": identity,
                   "sha256": hashlib.sha256(body).hexdigest()},
        )
        sources[identity] = (customers.limit(1).withColumn("full_name", F.lit(f"Alice {day}"))
                             .withColumn("source_batch_id", F.lit(identity))
                             .withColumn("source_batch_at", F.to_timestamp(
                                 F.lit(f"2026-03-{day:02d}T00:00:00Z"))))

    failed = False
    observed = {}

    def stage(service, selected):
        nonlocal failed
        if service != "spark-commerce-customer-scd2":
            return
        identity = selected["batch_id"]
        observed[identity] = selected["customer_history_mode"]
        retained = sources["a" * 16]
        if selected["customer_history_mode"] == "rebuild":
            for other, frame in sources.items():
                if other != "a" * 16:
                    retained = retained.unionByName(frame)
            history.rebuild_customer_history(spark, retained)
        else:
            apply_customer_history(spark, sources[identity], batch_id=identity)
        if identity == "c" * 16 and not failed:
            land("b" * 16, 2)
            failed = True
            raise RuntimeError("injected exit after newer history publication")

    def cycle(planner):
        # Exercise queue/history boundaries here; unit tests cover all runner stages/gold.
        selected = planner.plan(max_batches=1)["batches"]
        if not selected:
            return {"status": "idle"}
        batch = selected[0]
        planner.begin(batch)
        stage("spark-commerce-customer-scd2", {**batch, **planner.execution_options(batch)})
        return planner.commit(batch["batch_id"],
                              expected_manifest_sha256=batch["manifest_sha256"])

    with TemporaryDirectory(prefix="commerce-inflight-") as directory:
        state = Path(directory) / "checkpoint.json"
        planner = CommerceBatchPlanner(Landing(), bucket="acceptance", state_path=state)
        land("a" * 16, 1)
        assert cycle(planner)["batch_id"] == "a" * 16
        before = state.read_bytes()
        land("c" * 16, 3)
        snapshots = spark.table(f"{table}.snapshots").count()
        try:
            cycle(planner)
        except RuntimeError as error:
            assert "injected exit" in str(error)
        else:
            raise AssertionError("failure after history publication was not injected")
        assert state.read_bytes() == before
        assert spark.table(f"{table}.snapshots").count() == snapshots + 1
        planner = CommerceBatchPlanner(Landing(), bucket="acceptance", state_path=state)
        assert cycle(planner)["batch_id"] == "c" * 16
        assert spark.table(f"{table}.snapshots").count() == snapshots + 1
        assert cycle(planner)["batch_id"] == "b" * 16
        land("d" * 16, 4)
        assert cycle(planner)["batch_id"] == "d" * 16
        assert cycle(planner)["status"] == "idle"
        assert observed == {"a" * 16: "forward", "b" * 16: "rebuild",
                            "c" * 16: "forward", "d" * 16: "forward"}
        rows = spark.sql(f"SELECT full_name, valid_from, valid_to, is_current "
                         f"FROM {table} ORDER BY valid_from").collect()
        assert [row.full_name for row in rows] == [f"Alice {day}" for day in range(1, 5)]
        assert [row.is_current for row in rows] == [False, False, False, True]
        assert [row.valid_from.day for row in rows] == [1, 2, 3, 4]
        assert [row.valid_to.day for row in rows[:-1]] == [2, 3, 4]
        validate_target(spark)


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
