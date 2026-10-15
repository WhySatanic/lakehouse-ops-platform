from __future__ import annotations

import runpy
from contextlib import ExitStack
from pathlib import Path

import boto3
import pytest
from botocore.stub import Stubber


@pytest.mark.parametrize("fault", [None, "payload", "latest", "marker", "permissions"])
def test_minio_exercise_requires_versions_totals_pagination_and_denials(fault, monkeypatch):
    module = runpy.run_path(str(Path(__file__).parent / "integration" /
                               "test_warehouse_inventory_minio.py"))
    monkeypatch.setitem(module["exercise"].__globals__, "uuid4",
                        lambda: type("Identity", (), {"hex": "offline"})())
    prefix = "warehouse/inventory-offline/"
    live, deleted, metadata = [prefix + suffix for suffix in (
        "data/live.bin", "data/deleted.bin", "metadata/example.json",
    )]
    clients = [boto3.client("s3", region_name="us-east-1", aws_access_key_id="test",
                            aws_secret_access_key="test") for _ in range(2)]
    with ExitStack() as stack:
        writer, observer = [stack.enter_context(Stubber(client)) for client in clients]
        for index, (key, body) in enumerate(((live, b"aaa"), (live, b"bbbbb"),
                                            (deleted, b"ccccccc"), (metadata, b"{}"))):
            writer.add_response("put_object", {"VersionId": str(index)},
                                {"Bucket": "lakehouse", "Key": key, "Body": body})
        writer.add_response("delete_object", {"VersionId": "marker",
                                               "DeleteMarker": fault != "marker"},
                            {"Bucket": "lakehouse", "Key": deleted})
        baseline = {"Key": "warehouse/other/data/retained.bin", "Size": 12,
                    "VersionId": "old", "IsLatest": True}
        params = {"Bucket": "lakehouse", "Prefix": "warehouse/", "MaxKeys": 2}
        observer.add_response("get_bucket_versioning", {"Status": "Enabled"},
                              {"Bucket": "lakehouse"})
        observer.add_response("list_object_versions", {"Versions": [baseline],
                                                        "IsTruncated": False}, params)
        observer.add_response("get_bucket_versioning", {"Status": "Enabled"},
                              {"Bucket": "lakehouse"})
        observer.add_response("list_object_versions", {
            "Versions": [baseline, {"Key": live, "VersionId": "0",
                                    "Size": 4 if fault == "payload" else 3,
                                    "IsLatest": fault == "latest"}],
            "IsTruncated": True, "NextKeyMarker": live, "NextVersionIdMarker": "0",
        }, params)
        observer.add_response("list_object_versions", {
            "Versions": [{"Key": live, "VersionId": "1", "Size": 5, "IsLatest": True},
                         {"Key": deleted, "VersionId": "2", "Size": 7, "IsLatest": False}],
            "IsTruncated": True, "NextKeyMarker": deleted, "NextVersionIdMarker": "2",
        }, {**params, "KeyMarker": live, "VersionIdMarker": "0"})
        observer.add_response("list_object_versions", {
            "Versions": [{"Key": metadata, "VersionId": "3", "Size": 2, "IsLatest": True}],
            "DeleteMarkers": [{"Key": deleted, "VersionId": "marker", "IsLatest": True}],
            "IsTruncated": False,
        }, {**params, "KeyMarker": deleted, "VersionIdMarker": "2"})
        for name, request in (
            ("list_object_versions", {"Prefix": "landing/"}),
            ("list_object_versions", {"Prefix": ""}),
            ("get_object", {"Key": live}),
            ("put_object", {"Key": prefix + "forbidden.bin", "Body": b"x"}),
            ("delete_object", {"Key": live}),
        ):
            expected = {"Bucket": "lakehouse", **request}
            if name == "put_object" and fault == "permissions":
                observer.add_response(name, {}, expected)
            else:
                observer.add_client_error(name, service_error_code="AccessDenied",
                                          http_status_code=403, expected_params=expected)
        report = {"status": "failed"}
        if fault:
            with pytest.raises(AssertionError):
                module["exercise"](*clients, "lakehouse", report)
            assert report["status"] == "failed"
        else:
            module["exercise"](*clients, "lakehouse", report)
            assert report["status"] == "ready"
            assert report["pages"] == 3
            assert report["before"]["total_payload_bytes"] == 12
            assert report["after"]["total_payload_bytes"] == 29
            assert len(report["denied_operations"]) == 5
            writer.assert_no_pending_responses()
            observer.assert_no_pending_responses()
    for client in clients:
        client.close()


def test_failed_setup_retains_report_without_claiming_observation(tmp_path, monkeypatch):
    import json

    module = runpy.run_path(str(Path(__file__).parent / "integration" /
                               "test_warehouse_inventory_minio.py"))
    report_path = tmp_path / "evidence" / "inventory.json"
    monkeypatch.setenv("WAREHOUSE_INVENTORY_REPORT", str(report_path))
    monkeypatch.setenv("WAREHOUSE_INVENTORY_ACCESS_KEY_ID", "observer")
    monkeypatch.setenv("WAREHOUSE_INVENTORY_SECRET_ACCESS_KEY", "development")

    def unavailable(*args, **kwargs):
        raise RuntimeError("test setup unavailable")

    monkeypatch.setattr(boto3, "client", unavailable)
    with pytest.raises(RuntimeError, match="unavailable"):
        module["test_real_minio_warehouse_versions"]()
    assert json.loads(report_path.read_text())["status"] == "failed"
