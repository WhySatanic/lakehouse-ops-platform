from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_orphan_removal.py REPORT.json")
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    assert report["schema_version"] == "1.0"
    assert report["status"] == "succeeded"
    assert report["action_type"] == "inspect_orphan_files"
    assert report["applied"] is True
    assert report["before"] == report["after"]
    assert report["procedure_result"] == {
        "deleted_orphan_file_count": 1,
        "orphan_file_count": 1,
    }
    assert len(report["candidate_files"]) == 1
    assert report["candidate_set_id"].startswith("orphans-")
    bucket, key = _s3_parts(report["candidate_files"][0])
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("LAKEOPS_S3_ENDPOINT_URL", "http://localhost:9000"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "lakeops"),
        aws_secret_access_key=os.environ.get(
            "MINIO_ROOT_PASSWORD", "lakeops-development-only"
        ),
    )
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        assert error.response["Error"]["Code"] in {"404", "NoSuchKey"}
    else:
        raise AssertionError("approved orphan object still exists")
    print(
        json.dumps(
            {
                "candidate_set_id": report["candidate_set_id"],
                "deleted_orphan_file_count": 1,
                "status": "ready",
            },
            sort_keys=True,
        )
    )


def _s3_parts(location: str) -> tuple[str, str]:
    scheme, remainder = location.split("://", 1)
    assert scheme in {"s3", "s3a", "s3n"}
    bucket, key = remainder.split("/", 1)
    return bucket, key


if __name__ == "__main__":
    main()
