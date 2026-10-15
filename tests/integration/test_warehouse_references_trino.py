from __future__ import annotations

import json
import os
from pathlib import Path

import boto3
import pytest
from botocore.config import Config

from lakehouse_ops.commerce_pipeline_profile import _INVENTORY_TABLES_SQL
from lakehouse_ops.trino import TrinoClient
from lakehouse_ops.warehouse_inventory import warehouse_inventory
from lakehouse_ops.warehouse_references import file_references, reconcile_references


def exercise(client, observer, report):
    tables = client.query(_INVENTORY_TABLES_SQL)
    assert tables
    report["references"] = references = file_references(client, tables)
    assert any(len(table["snapshots"]) > 1 for table in references)
    assert any(table["current"] for table in references)
    assert any(len(table["retained"]) > len(table["current"]) for table in references)
    report["warehouse"] = storage = warehouse_inventory(observer)
    if (client.query(_INVENTORY_TABLES_SQL) != tables
            or file_references(client, tables) != references):
        raise ValueError("Iceberg references changed during warehouse listing")
    report["reference_accounting"] = reconcile_references(storage, references)
    report["status"] = "ready"


@pytest.mark.skipif(os.getenv("LAKEOPS_RUN_RETAINED_REFERENCES") != "1",
                    reason="requires the completed serving CI commerce tables")
def test_current_and_retained_file_references_on_real_trino():
    output = Path("artifacts/warehouse-references-trino.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {"status": "failed", "scope": "commerce_file_references_and_warehouse_payload",
              "source_sha": os.getenv("GITHUB_SHA")}
    observer = None
    try:
        observer = boto3.client(
            "s3", endpoint_url=os.getenv("LAKEOPS_S3_ENDPOINT_URL", "http://localhost:9000"),
            region_name="us-east-1",
            aws_access_key_id=os.environ["WAREHOUSE_INVENTORY_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["WAREHOUSE_INVENTORY_SECRET_ACCESS_KEY"],
            config=Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 1}),
        )
        with TrinoClient(server="http://localhost:8080", user="lakehouse-ci",
                         query_timeout=30) as client:
            exercise(client, observer, report)
    finally:
        try:
            output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        finally:
            if observer is not None:
                observer.close()
