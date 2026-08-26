from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

import boto3


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "usage: prepare_orphan_removal_fixture.py PLAN INSPECTION OUTPUT"
        )
    plan = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    report = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
    output = Path(sys.argv[3])
    action = next(
        action
        for action in plan["actions"]
        if action["action_type"] == "inspect_orphan_files"
    )
    bucket = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
    key = "warehouse/ops/maintenance_fixture/data/orphan-removal-fixture.parquet"
    location = f"s3a://{bucket}/{key}"
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("LAKEOPS_S3_ENDPOINT_URL", "http://localhost:9000"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("MINIO_SPARK_USER", "lakeops-spark"),
        aws_secret_access_key=os.environ.get(
            "MINIO_SPARK_PASSWORD", "spark-development-only"
        ),
    )
    client.put_object(Bucket=bucket, Key=key, Body=b"orphan-removal-fixture")
    candidate_files = [location]
    candidate_set_id = _candidate_set_id(
        plan["table"], action["parameters"]["older_than"], candidate_files
    )
    report["candidate_files"] = candidate_files
    report["candidate_set_id"] = candidate_set_id
    report["procedure_result"]["orphan_file_count"] = 1
    output.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "candidate_set_id": candidate_set_id,
                "location": location,
                "status": "ready",
            },
            sort_keys=True,
        )
    )


def _candidate_set_id(table: str, older_than: str, files: list[str]) -> str:
    value = json.dumps(
        {"table": table, "older_than": older_than, "files": files},
        sort_keys=True,
        separators=(",", ":"),
    )
    return "orphans-" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


if __name__ == "__main__":
    main()
