from __future__ import annotations

from pathlib import Path
from typing import Any

from lakehouse_ops.doctor import (
    DoctorReport,
    check_file_landing,
    check_s3_bucket,
    check_s3_versioning,
)


class AvailableBucketClient:
    def get_bucket_location(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs == {"Bucket": "lakehouse"}
        return {}


class UnavailableBucketClient:
    def get_bucket_location(self, **kwargs: Any) -> dict[str, Any]:
        raise ConnectionError("endpoint unavailable")


class VersionedBucketClient:
    def __init__(self, status: str | None = "Enabled") -> None:
        self.status = status

    def get_bucket_versioning(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs == {"Bucket": "lakehouse"}
        return {"Status": self.status} if self.status else {}


def test_file_landing_check_writes_and_removes_probe(tmp_path: Path) -> None:
    landing = tmp_path / "landing"

    result = check_file_landing(landing)

    assert result.status == "passed"
    assert result.target == str(landing.resolve())
    assert list(landing.iterdir()) == []


def test_file_landing_check_reports_unusable_path(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.write_text("not a directory", encoding="utf-8")

    result = check_file_landing(landing)

    assert result.status == "failed"
    assert result.name == "file_landing_write"
    assert "FileExistsError" in result.message


def test_s3_bucket_check_reports_access() -> None:
    result = check_s3_bucket(AvailableBucketClient(), "lakehouse")

    assert result.status == "passed"
    assert result.target == "s3://lakehouse"


def test_s3_bucket_check_turns_connection_error_into_failed_report() -> None:
    result = check_s3_bucket(UnavailableBucketClient(), "lakehouse")
    report = DoctorReport((result,))

    assert report.healthy is False
    assert report.as_dict()["status"] == "failed"
    assert "endpoint unavailable" in result.message


def test_s3_versioning_check_requires_enabled_status() -> None:
    enabled = check_s3_versioning(VersionedBucketClient(), "lakehouse")
    suspended = check_s3_versioning(VersionedBucketClient("Suspended"), "lakehouse")
    unset = check_s3_versioning(VersionedBucketClient(None), "lakehouse")

    assert enabled.status == "passed"
    assert enabled.message == "bucket versioning is enabled"
    assert suspended.status == "failed"
    assert "Suspended" in suspended.message
    assert unset.status == "failed"
    assert "unset" in unset.message


def test_s3_versioning_check_reports_api_failure() -> None:
    class FailingVersioningClient:
        def get_bucket_versioning(self, **kwargs: Any) -> dict[str, Any]:
            raise PermissionError("versioning status denied")

    result = check_s3_versioning(FailingVersioningClient(), "lakehouse")

    assert result.status == "failed"
    assert "versioning status denied" in result.message
