from __future__ import annotations

import runpy
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber


@pytest.mark.parametrize("fault", [None, "size", "missing", "listing", "race", "tables"])
def test_real_exercise_requires_catalog_and_sdk_payload_to_agree(fault):
    module = runpy.run_path(str(Path(__file__).parent / "integration" /
                               "test_warehouse_references_trino.py"))
    current = {"file_path": "s3://lakehouse/warehouse/t/data/live", "bytes": 10, "content": 0}
    old = {"file_path": "s3://lakehouse/warehouse/t/data/old", "bytes": 7, "content": 0}

    class Catalog:
        snapshots = 0
        table_reads = 0

        def query(self, sql):
            if "information_schema" in sql:
                self.table_reads += 1
                if fault == "tables" and self.table_reads > 1:
                    return []
                return [{"table_schema": "silver", "table_name": "commerce_customers"}]
            if "$snapshots" in sql:
                self.snapshots += 1
                return [{"snapshot_id": 1}, {"snapshot_id": (
                    3 if fault == "race" and self.snapshots > 1 else 2
                )}]
            if "$all_entries" in sql:
                return [current, old]
            assert "$files" in sql
            return [current]

    observer = boto3.client("s3", region_name="us-east-1", aws_access_key_id="observer",
                            aws_secret_access_key="test")
    report = {"status": "failed"}
    try:
        with Stubber(observer) as stub:
            stub.add_response("get_bucket_versioning", {"Status": "Enabled"},
                              {"Bucket": "lakehouse"})
            params = {"Bucket": "lakehouse", "Prefix": "warehouse/"}
            if fault == "listing":
                stub.add_client_error("list_object_versions", "AccessDenied", http_status_code=403,
                                      expected_params=params)
            else:
                objects = [
                    {"Key": "warehouse/t/data/live", "Size": 9 if fault == "size" else 10,
                     "VersionId": "live", "IsLatest": True},
                    {"Key": "warehouse/t/data/old", "Size": 7, "VersionId": "old",
                     "IsLatest": fault != "missing"},
                    {"Key": "warehouse/t/metadata/a", "Size": 4, "VersionId": "metadata",
                     "IsLatest": True},
                ]
                stub.add_response("list_object_versions", {"Versions": objects,
                                                            "IsTruncated": False}, params)
            if fault:
                with pytest.raises((ValueError, ClientError),
                                   match=r"missing|differs|changed|AccessDenied"):
                    module["exercise"](Catalog(), observer, report)
                assert report["status"] == "failed"
            else:
                module["exercise"](Catalog(), observer, report)
                assert report["status"] == "ready"
                accounting = report["reference_accounting"]
                assert accounting["total_payload_bytes"] == 21
                assert accounting["groups"]["current_data"]["bytes"] == 10
                assert accounting["groups"]["retained_only_data"]["bytes"] == 7
                assert accounting["groups"]["latest_not_in_selected_file_references"]["bytes"] == 4
                assert accounting["orphan_classification"] == "not_performed"
            stub.assert_no_pending_responses()
    finally:
        observer.close()
