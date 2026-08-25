from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

from lakehouse_ops.trino import TrinoClient

EXPECTED_ROWS = [[1, "committed-a"], [2, "committed-b"], [3, "committed-c"]]
EXPECTED_ERROR = "injected interruption after data upload and before metadata commit"


class InjectedWriteInterruption(RuntimeError):
    pass


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: exercise_interrupted_write_reconciliation.py REPORT")
    report_path = Path(sys.argv[1])
    report = json.loads(report_path.read_text(encoding="utf-8"))
    _validate_baseline(report)
    bucket, source_key = _s3_parts(report["source_file"])
    candidate_key = (
        "warehouse/ops/interrupted_write_fixture/data/"
        f"interrupted-before-commit-{report['before']['snapshot_id']}.parquet"
    )
    candidate_location = f"s3a://{bucket}/{candidate_key}"
    s3 = _s3_client()
    _require_missing(s3, bucket, candidate_key)

    try:
        s3.copy_object(
            Bucket=bucket,
            Key=candidate_key,
            CopySource={"Bucket": bucket, "Key": source_key},
        )
        raise InjectedWriteInterruption(EXPECTED_ERROR)
    except InjectedWriteInterruption as error:
        injected_error = str(error)

    uploaded = s3.head_object(Bucket=bucket, Key=candidate_key)
    with TrinoClient(
        os.environ.get("TRINO_SERVER", "http://localhost:8080"),
        user="lakehouse-recovery-drill",
    ) as trino:
        interrupted = _table_state(trino, report["table"])
        _require_unchanged(report["before"], interrupted)
        if candidate_location in interrupted["referenced_files"]:
            raise RuntimeError("injected object unexpectedly became an Iceberg data file")

        approved_etag = _etag(uploaded)
        current = s3.head_object(Bucket=bucket, Key=candidate_key)
        if _etag(current) != approved_etag:
            raise RuntimeError("injected object changed after reconciliation approval")
        s3.delete_object(Bucket=bucket, Key=candidate_key)
        _require_missing(s3, bucket, candidate_key)

        reconciled = _table_state(trino, report["table"])
        _require_unchanged(report["before"], reconciled)

    report.update(
        {
            "status": "recovered",
            "injection_point": "after_data_upload_before_metadata_commit",
            "injected_error": injected_error,
            "interrupted": {
                **interrupted,
                "candidate": {
                    "location": candidate_location,
                    "etag": approved_etag,
                    "size_bytes": int(uploaded["ContentLength"]),
                    "exists": True,
                    "referenced": False,
                },
            },
            "reconciled": {**reconciled, "candidate_exists": False},
            "post_conditions": {
                "snapshot_unchanged": True,
                "rows_unchanged": True,
                "referenced_files_unchanged": True,
                "exact_candidate_removed": True,
            },
        }
    )
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))


def _table_state(trino: TrinoClient, table: str) -> dict[str, Any]:
    catalog, schema, name = table.split(".", 2)
    snapshot_rows = trino.query(
        f'SELECT snapshot_id FROM "{catalog}"."{schema}"."{name}$refs" '
        "WHERE name = 'main'"
    )
    if len(snapshot_rows) != 1:
        raise RuntimeError("expected one main snapshot reference")
    rows = trino.query(
        f'SELECT event_id, payload FROM "{catalog}"."{schema}"."{name}" '
        "ORDER BY event_id"
    )
    files = trino.query(
        f'SELECT file_path FROM "{catalog}"."{schema}"."{name}$files" '
        "WHERE content = 0 ORDER BY file_path"
    )
    return {
        "snapshot_id": str(snapshot_rows[0]["snapshot_id"]),
        "rows": [[int(row["event_id"]), str(row["payload"])] for row in rows],
        "referenced_files": [str(row["file_path"]) for row in files],
    }


def _validate_baseline(report: dict[str, Any]) -> None:
    if report.get("schema_version") != "1.0":
        raise RuntimeError("unsupported interrupted-write report schema")
    if report.get("status") != "baseline_ready":
        raise RuntimeError("interrupted-write baseline is not ready")
    if report.get("scenario") != "interrupted_write_before_metadata_commit":
        raise RuntimeError("unexpected failure-injection scenario")
    if report.get("table") != "lakehouse.ops.interrupted_write_fixture":
        raise RuntimeError("unexpected failure-injection table")
    if report.get("before", {}).get("rows") != EXPECTED_ROWS:
        raise RuntimeError("unexpected baseline rows")
    if report.get("source_file") not in report["before"].get("referenced_files", []):
        raise RuntimeError("copy source is not referenced by the baseline snapshot")


def _require_unchanged(expected: dict[str, Any], actual: dict[str, Any]) -> None:
    if actual != expected:
        raise RuntimeError(
            "Iceberg state changed across interrupted write: "
            f"expected={expected}, actual={actual}"
        )


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("LAKEOPS_S3_ENDPOINT_URL", "http://localhost:9000"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "lakeops"),
        aws_secret_access_key=os.environ.get(
            "MINIO_ROOT_PASSWORD", "lakeops-development-only"
        ),
    )


def _require_missing(client, bucket: str, key: str) -> None:
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response["Error"]["Code"] not in {"404", "NoSuchKey", "NotFound"}:
            raise
    else:
        raise RuntimeError(f"object must not exist: s3a://{bucket}/{key}")


def _etag(response: dict[str, Any]) -> str:
    value = str(response.get("ETag", "")).strip('"')
    if not value:
        raise RuntimeError("S3 response has no ETag")
    return value


def _s3_parts(location: str) -> tuple[str, str]:
    scheme, remainder = location.split("://", 1)
    if scheme not in {"s3", "s3a", "s3n"}:
        raise RuntimeError(f"unsupported object-store scheme: {scheme}")
    bucket, key = remainder.split("/", 1)
    return bucket, key


if __name__ == "__main__":
    main()
