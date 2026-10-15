"""Exercise production bronze JSONL/schema guards without a catalog or warehouse writes."""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from bronze_commerce import TABLES, prepare_source

from lakehouse_ops.ingestion.commerce_bronze import CommerceBatchDirectory, CommerceTableFile

RECORDS = {
    "customers": {"customer_id": "c1", "full_name": "Alice", "email": None,
                  "registered_at": "2026-01-01T00:00:00Z"},
    "orders": {"order_id": "o1", "customer_id": "c1", "product_id": "p1",
               "quantity": 2, "unit_price_cents": 125, "total_cents": 250,
               "event_at": "2026-01-01T00:00:00Z", "ingested_at": "2026-01-02T00:00:00Z"},
    "payments": {"payment_id": "t1", "order_id": "o1", "amount_cents": None,
                 "status": "captured", "paid_at": "2026-01-01T00:00:00Z"},
    "products": {"product_id": "p1", "name": "Product", "category": "Test",
                 "unit_price_cents": 125},
}
TIMESTAMPS = {"registered_at", "event_at", "ingested_at", "paid_at"}


def table_file(root, name, records):
    path = root / f"{name}.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in records), encoding="utf-8")
    return CommerceTableFile(name, path, len(records),
                             hashlib.sha256(path.read_bytes()).hexdigest())


def require_rejection(spark, batch, selected, message):
    try:
        prepare_source(spark, batch, selected)
    except Exception as error:
        assert message in str(error), str(error)
    else:
        raise AssertionError(f"expected rejection: {message}")


def check(spark):
    cases = 0
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for name, record in RECORDS.items():
            selected = table_file(root, name, [record, record])
            batch = CommerceBatchDirectory("a" * 16, "2026-01-01T00:00:00Z", (selected,))
            rows = prepare_source(spark, batch, selected).collect()
            assert len(rows) == 2
            assert sorted(row.source_row_occurrence for row in rows) == [1, 2]
            assert len({row.source_row_hash for row in rows}) == 1
            assert all(re.fullmatch(r"[0-9a-f]{64}", row.source_row_hash) for row in rows)
            assert all(row.batch_id == batch.batch_id for row in rows)
            assert all(row.source_file_sha256 == selected.sha256 for row in rows)
            assert all(row.source_batch_at == datetime(2026, 1, 1) for row in rows)
            for column, value in record.items():
                expected = (datetime.fromisoformat(value.replace("Z", "+00:00"))
                            .replace(tzinfo=None) if column in TIMESTAMPS else value)
                assert all(row[column] == expected for row in rows), (name, column)
            cases += 1
            require_rejection(spark, batch, replace(selected, rows=3),
                              f"Spark row count mismatch for {name}: expected 3, observed 2")
            cases += 1

            for column in TABLES[name].required:
                invalid = table_file(root, name, [{**record, column: None}])
                require_rejection(spark, batch, invalid,
                                  f"required values are missing in 1 {name} row(s)")
                cases += 1

            # Count invalid rows, not the number of missing fields in each row.
            invalid = table_file(root, name, [{**record, **dict.fromkeys(TABLES[name].required)}])
            require_rejection(spark, batch, invalid,
                              f"required values are missing in 1 {name} row(s)")
            invalid = table_file(root, name, [{**record, TABLES[name].required[0]: None}])
            require_rejection(spark, batch, replace(invalid, rows=2),
                              f"Spark row count mismatch for {name}: expected 2, observed 1")
            empty = table_file(root, name, [])
            require_rejection(spark, batch, replace(empty, rows=1),
                              f"Spark row count mismatch for {name}: expected 1, observed 0")
            cases += 3

        mistyped = table_file(root, "orders", [{**RECORDS["orders"], "quantity": "not-an-integer"}])
        require_rejection(spark, batch, mistyped, "MALFORMED")
        malformed = table_file(root, "products", [RECORDS["products"]])
        malformed.path.write_text("not-json\n", encoding="utf-8")
        require_rejection(spark, batch, malformed, "MALFORMED")
        cases += 2
    report = {"kind": "commerce_bronze_validation", "status": "ready", "cases": cases,
              "scope": "jsonl_schema_and_required_fields_before_merge", "tables": sorted(RECORDS)}
    print(json.dumps(report, sort_keys=True))
    return report
