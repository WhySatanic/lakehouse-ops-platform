from __future__ import annotations

import json

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from lakehouse_ops.warehouse_inventory import warehouse_inventory


def version(key, size, *, latest=True, identity="null"):
    return {"Key": "warehouse/" + key, "Size": size,
            "IsLatest": latest, "VersionId": identity}


@pytest.mark.parametrize("status", [None, "Enabled", "Suspended"])
def test_real_sdk_pagination_includes_hidden_versions_and_metadata(status):
    client = boto3.client("s3", region_name="us-east-1",
                          aws_access_key_id="test", aws_secret_access_key="test")
    with Stubber(client) as stub:
        stub.add_response("get_bucket_versioning", {"Status": status} if status else {},
                          {"Bucket": "lakehouse"})
        stub.add_response("list_object_versions", {
            "IsTruncated": True, "NextKeyMarker": "warehouse/table/data/a.parquet",
            "NextVersionIdMarker": "v1",
            "Versions": [version("table/data/a.parquet", 100, identity="v2"),
                         version("table/data/a.parquet", 90, latest=False, identity="v1")],
        }, {"Bucket": "lakehouse", "Prefix": "warehouse/"})
        stub.add_response("list_object_versions", {
            "IsTruncated": False,
            "Versions": [version("table/metadata/v1.json", 20),
                         version("table/data/deleted.parquet", 50, latest=False),
                         version("unknown.bin", 0)],
            "DeleteMarkers": [{"Key": "warehouse/table/data/deleted.parquet",
                               "VersionId": "deleted", "IsLatest": True}],
        }, {"Bucket": "lakehouse", "Prefix": "warehouse/",
            "KeyMarker": "warehouse/table/data/a.parquet", "VersionIdMarker": "v1"})
        report = warehouse_inventory(client)
        stub.assert_no_pending_responses()
    client.close()
    assert report["total_payload_bytes"] == 260
    assert report["versions"] == {"latest": {"objects": 3, "bytes": 120},
                                   "noncurrent": {"objects": 2, "bytes": 140}}
    assert report["layout"] == {"metadata": {"objects": 1, "bytes": 20},
                                "data": {"objects": 3, "bytes": 240},
                                "other": {"objects": 1, "bytes": 0}}
    assert report["delete_marker_count"] == 1
    assert len(report["objects"]) == 5
    assert report["bucket_versioning"] == (status or "Disabled")
    assert report["iceberg_reachability"] == "not_classified"
    assert json.loads(json.dumps(report)) == report


class Client:
    def __init__(self, pages):
        self.pages = pages

    def get_bucket_versioning(self, **kwargs):
        return {}

    def get_paginator(self, name):
        assert name == "list_object_versions"
        return self

    def paginate(self, **kwargs):
        assert kwargs == {"Bucket": "lakehouse", "Prefix": "warehouse/"}
        yield from self.pages


def test_empty_warehouse_has_zero_payload():
    report = warehouse_inventory(Client([{}]))
    assert report["total_payload_bytes"] == report["delete_marker_count"] == 0
    assert report["objects"] == report["delete_markers"] == []


@pytest.mark.parametrize("patch", [{"Key": "landing/file"}, {"IsLatest": 1},
                                   {"Size": -1}, {"Size": True}])
def test_malformed_or_out_of_scope_listing_is_rejected(patch):
    row = {**version("a", 10), **patch}
    with pytest.raises(ValueError):
        warehouse_inventory(Client([{"Versions": [row]}]))


def test_duplicate_across_pages_is_rejected():
    page = {"Versions": [version("a", 10)]}
    with pytest.raises(ValueError, match="duplicate"):
        warehouse_inventory(Client([page, page]))


def test_failure_after_a_page_does_not_return_partial_totals():
    class Broken(Client):
        def paginate(self, **kwargs):
            yield {"Versions": [version("a", 10)]}
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "ListObjectVersions")

    with pytest.raises(ClientError, match="AccessDenied"):
        warehouse_inventory(Broken([]))


@pytest.mark.parametrize("credentials", [False, True])
@pytest.mark.parametrize("fail", [False, True])
def test_profile_cli_uses_separate_credentials_and_closes_client(
    tmp_path, monkeypatch, credentials, fail,
):
    from lakehouse_ops import commerce_pipeline_profile as profile

    monkeypatch.setattr("sys.argv", ["profile", "--output", str(tmp_path),
                                    "--s3-endpoint-url", "http://localhost:19100",
                                    "--server", "http://localhost:18100",
                                    "--warehouse-inventory"])
    for key in ("WAREHOUSE_INVENTORY_ACCESS_KEY_ID", "WAREHOUSE_INVENTORY_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(key, raising=False)
    if credentials:
        monkeypatch.setenv("WAREHOUSE_INVENTORY_ACCESS_KEY_ID", "observer")
        monkeypatch.setenv("WAREHOUSE_INVENTORY_SECRET_ACCESS_KEY", "separate-secret")
    calls = []

    class Observer:
        def close(self):
            calls.append("closed")

    observer = Observer()

    def factory(service, **kwargs):
        assert credentials
        assert service == "s3"
        assert kwargs["aws_access_key_id"] == "observer"
        assert kwargs["aws_secret_access_key"] == "separate-secret"
        assert kwargs["endpoint_url"] == "http://localhost:19100"
        assert kwargs["config"].read_timeout == 30
        calls.append("created")
        return observer

    def run(*args, **kwargs):
        assert kwargs["warehouse_client"] is observer
        calls.append("run")
        if fail:
            raise ValueError("profile failed")
        return {"status": "ready"}

    monkeypatch.setattr(profile.boto3, "client", factory)
    monkeypatch.setattr(profile, "profile", run)
    assert profile.main() == (2 if fail or not credentials else 0)
    assert calls == (["created", "run", "closed"] if credentials else [])
