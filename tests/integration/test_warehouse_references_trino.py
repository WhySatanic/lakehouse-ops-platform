from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from lakehouse_ops.commerce_pipeline_profile import _INVENTORY_TABLES_SQL
from lakehouse_ops.trino import TrinoClient
from lakehouse_ops.warehouse_references import file_references


@pytest.mark.skipif(os.getenv("LAKEOPS_RUN_RETAINED_REFERENCES") != "1",
                    reason="requires the completed serving CI commerce tables")
def test_current_and_retained_file_references_on_real_trino():
    output = Path("artifacts/warehouse-references-trino.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {"status": "failed", "scope": "catalog_references_only"}
    try:
        with TrinoClient(server="http://localhost:8080", user="lakehouse-ci") as client:
            tables = client.query(_INVENTORY_TABLES_SQL)
            assert tables
            report["references"] = references = file_references(client, tables)
            assert any(len(table["snapshots"]) > 1 for table in references)
            assert any(table["current"] for table in references)
            assert any(len(table["retained"]) > len(table["current"]) for table in references)
        report["status"] = "ready"
    finally:
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")
