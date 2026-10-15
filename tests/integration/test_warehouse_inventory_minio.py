"""Opt-in versioned MinIO exercise; writes only a fresh UUID namespace and retains it."""

from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from lakehouse_ops.warehouse_inventory import warehouse_inventory


class SmallPages:
    """Use the actual SDK paginator with two entries per response, not a fake page loop."""

    def __init__(self, client):
        self.client = client
        self.pages = 0

    def get_bucket_versioning(self, **kwargs):
        return self.client.get_bucket_versioning(**kwargs)

    def get_paginator(self, name):
        self.paginator = self.client.get_paginator(name)
        return self

    def paginate(self, **kwargs):
        self.pages = 0
        for page in self.paginator.paginate(**kwargs, PaginationConfig={"PageSize": 2}):
            self.pages += 1
            yield page


def require_denied(operation, **kwargs):
    try:
        result = operation(**kwargs)
    except ClientError as error:
        assert error.response["Error"]["Code"] == "AccessDenied"
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    else:
        # Close any unexpected successful GetObject stream before failing the check.
        if "Body" in result:
            result["Body"].close()
        raise AssertionError("inventory observer exceeded its listing-only permissions")


def exercise(writer, observer, bucket, report):
    listing = SmallPages(observer)
    before = warehouse_inventory(listing, bucket=bucket)
    assert before["bucket_versioning"] == "Enabled"
    report["before"] = before
    prefix = f"warehouse/inventory-{uuid4().hex}/"
    report["owned_prefix"] = prefix
    live, deleted, metadata = (prefix + suffix for suffix in (
        "data/live.bin", "data/deleted.bin", "metadata/example.json",
    ))
    written = []
    for key, body in ((live, b"aaa"), (live, b"bbbbb"),
                      (deleted, b"ccccccc"), (metadata, b"{}")):
        version = writer.put_object(Bucket=bucket, Key=key, Body=body)["VersionId"]
        assert version and version != "null"
        written.append({"key": key, "version_id": version, "bytes": len(body)})
    marker = writer.delete_object(Bucket=bucket, Key=deleted)
    assert marker["DeleteMarker"] is True
    report["written"] = written
    report["delete_marker"] = {"key": deleted, "version_id": marker["VersionId"]}
    after = warehouse_inventory(listing, bucket=bucket)
    report.update(after=after, pages=listing.pages)
    assert listing.pages >= 3
    records = [row for row in after["objects"] if row["key"].startswith(prefix)]
    assert {(row["key"], row["version_id"], row["bytes"]) for row in records} == {
        (row["key"], row["version_id"], row["bytes"]) for row in written
    }
    assert sum(row["bytes"] for row in records if row["is_latest"]) == 7
    assert sum(row["bytes"] for row in records if not row["is_latest"]) == 10
    assert after["total_payload_bytes"] - before["total_payload_bytes"] == 17
    assert after["delete_marker_count"] - before["delete_marker_count"] == 1
    assert [row for row in after["delete_markers"] if row["key"].startswith(prefix)] == [{
        "key": deleted, "version_id": marker["VersionId"], "is_latest": True,
    }]
    for operation, kwargs in (
        (observer.list_object_versions, {"Prefix": "landing/"}),
        (observer.list_object_versions, {"Prefix": ""}),
        (observer.get_object, {"Key": live}),
        (observer.put_object, {"Key": prefix + "forbidden.bin", "Body": b"x"}),
        (observer.delete_object, {"Key": live}),
    ):
        require_denied(operation, Bucket=bucket, **kwargs)
    report["denied_operations"] = ["list_landing", "list_bucket_root", "get", "put", "delete"]
    report["status"] = "ready"


@pytest.mark.skipif(os.getenv("LAKEOPS_RUN_WAREHOUSE_INVENTORY_SMOKE") != "1",
                    reason="requires initialized versioned MinIO and listing-only observer")
def test_real_minio_warehouse_versions():
    output = Path(os.environ["WAREHOUSE_INVENTORY_REPORT"])
    if output.exists():
        raise ValueError("requires a new inventory evidence path")
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {"status": "failed", "scope": "synthetic_versioned_payload_not_iceberg_storage"}
    clients = []
    try:
        for access, secret in ((os.getenv("MINIO_SPARK_USER", "lakeops-spark"),
                                 os.getenv("MINIO_SPARK_PASSWORD", "spark-development-only")),
                                (os.environ["WAREHOUSE_INVENTORY_ACCESS_KEY_ID"],
                                 os.environ["WAREHOUSE_INVENTORY_SECRET_ACCESS_KEY"])):
            clients.append(boto3.client(
                "s3", endpoint_url=os.getenv("LAKEOPS_S3_ENDPOINT_URL", "http://localhost:9000"),
                region_name="us-east-1", aws_access_key_id=access, aws_secret_access_key=secret,
                config=Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 1}),
            ))
        exercise(*clients, os.getenv("LAKEHOUSE_BUCKET", "lakehouse"), report)
    finally:
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        for client in clients:
            client.close()
