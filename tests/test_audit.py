from __future__ import annotations

import json
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops.ingestion.audit import (
    audit_file_landing,
    audit_s3_landing,
    audit_s3_landing_versions,
)
from lakehouse_ops.ingestion.landing import FileLandingZone
from lakehouse_ops.ingestion.models import Location, WeatherPayload


def land_payload(root: Path, source_payload: dict[str, Any]) -> Path:
    payload = WeatherPayload.from_source(Location("Moscow", 55.75, 37.62), source_payload)
    result = FileLandingZone(root).write(
        payload, ingested_at=datetime(2026, 8, 18, 10, 30, tzinfo=UTC)
    )
    assert isinstance(result.path, Path)
    return result.path


def test_audit_accepts_consistent_landing_object(
    tmp_path: Path, valid_source_payload: dict[str, Any]
) -> None:
    path = land_payload(tmp_path, valid_source_payload)

    report = audit_file_landing(tmp_path)

    assert report.healthy is True
    assert report.valid == 1
    assert report.invalid == 0
    assert report.items[0].path == path.relative_to(tmp_path).as_posix()
    assert report.items[0].errors == ()


@pytest.mark.parametrize("name", [None, 42, True, [], {}])
def test_audit_reports_invalid_location_name_and_continues(
    tmp_path: Path, valid_source_payload: dict[str, Any], name: object
) -> None:
    path = land_payload(tmp_path, valid_source_payload)
    document = json.loads(path.read_text(encoding="utf-8"))
    document["ingestion"]["location"]["name"] = name
    damaged = path.with_name("damaged.json")
    damaged.write_text(json.dumps(document), encoding="utf-8")

    report = audit_file_landing(tmp_path)

    assert report.healthy is False
    assert report.valid == 1
    assert report.invalid == 1
    item = next(item for item in report.items if item.status == "invalid")
    assert item.errors == ("invalid weather payload: ingestion location name must be a string",)
    assert damaged.read_text(encoding="utf-8") == json.dumps(document)


def test_audit_detects_payload_checksum_mismatch(
    tmp_path: Path, valid_source_payload: dict[str, Any]
) -> None:
    path = land_payload(tmp_path, valid_source_payload)
    document = json.loads(path.read_text(encoding="utf-8"))
    document["payload"]["hourly"]["temperature_2m"][0] = -99
    path.write_text(json.dumps(document), encoding="utf-8")

    report = audit_file_landing(tmp_path)

    assert report.healthy is False
    assert report.invalid == 1
    assert report.items[0].errors == (
        "declared checksum does not match payload",
        "filename checksum does not match payload",
    )


def test_audit_detects_invalid_json_and_layout(tmp_path: Path) -> None:
    path = tmp_path / "unexpected.json"
    path.write_text("{broken", encoding="utf-8")

    report = audit_file_landing(tmp_path)

    assert report.invalid == 1
    assert report.items[0].errors[0] == (
        "path must match source/date/location/checksum.json layout"
    )
    assert report.items[0].errors[1].startswith("cannot read JSON:")


def test_audit_rejects_empty_or_missing_landing(tmp_path: Path) -> None:
    empty = audit_file_landing(tmp_path)
    missing = audit_file_landing(tmp_path / "missing")

    assert empty.healthy is False
    assert empty.as_dict()["status"] == "failed"
    assert missing.healthy is False
    assert missing.items == ()


class FakeS3AuditClient:
    def __init__(self, objects: dict[str, tuple[bytes, dict[str, str]]]) -> None:
        self.objects = objects
        self.list_requests: list[dict[str, Any]] = []

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        self.list_requests.append(kwargs)
        keys = sorted(key for key in self.objects if key.startswith(kwargs.get("Prefix", "")))
        if "ContinuationToken" not in kwargs and len(keys) > 1:
            return {
                "Contents": [{"Key": keys[0]}],
                "IsTruncated": True,
                "NextContinuationToken": "page-2",
            }
        page = keys[1:] if "ContinuationToken" in kwargs else keys
        return {"Contents": [{"Key": key} for key in page], "IsTruncated": False}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        body, metadata = self.objects[kwargs["Key"]]
        return {"Body": BytesIO(body), "Metadata": metadata}


def test_audit_s3_landing_reads_paginated_prefix(
    tmp_path: Path, valid_source_payload: dict[str, Any]
) -> None:
    first = land_payload(tmp_path, valid_source_payload)
    document = json.loads(first.read_text(encoding="utf-8"))
    checksum = document["ingestion"]["object_checksum"]
    key = f"landing/{first.relative_to(tmp_path).as_posix()}"
    client = FakeS3AuditClient(
        {
            "landing/ignored.txt": (b"ignored", {}),
            key: (first.read_bytes(), {"sha256": checksum}),
        }
    )

    report = audit_s3_landing(client, bucket="lakehouse", prefix="/landing/")

    assert report.healthy is True
    assert report.root == "s3://lakehouse/landing"
    assert report.items[0].path == first.relative_to(tmp_path).as_posix()
    assert len(client.list_requests) == 2
    assert client.list_requests[1]["ContinuationToken"] == "page-2"


@pytest.mark.parametrize(
    ("metadata", "expected_error"),
    [
        ({}, "object metadata checksum is missing"),
        ({"sha256": "wrong"}, "object metadata checksum does not match payload"),
    ],
)
def test_audit_s3_landing_validates_checksum_metadata(
    tmp_path: Path,
    valid_source_payload: dict[str, Any],
    metadata: dict[str, str],
    expected_error: str,
) -> None:
    path = land_payload(tmp_path, valid_source_payload)
    key = f"landing/{path.relative_to(tmp_path).as_posix()}"
    client = FakeS3AuditClient({key: (path.read_bytes(), metadata)})

    report = audit_s3_landing(client, bucket="lakehouse", prefix="landing")

    assert report.healthy is False
    assert expected_error in report.items[0].errors


class FakeS3VersionAuditClient:
    def __init__(self, key: str, body: bytes, checksum: str) -> None:
        self.key = key
        self.body = body
        self.checksum = checksum
        self.list_requests: list[dict[str, Any]] = []
        self.get_requests: list[dict[str, Any]] = []

    def list_object_versions(self, **kwargs: Any) -> dict[str, Any]:
        self.list_requests.append(kwargs)
        if "KeyMarker" not in kwargs:
            return {
                "Versions": [
                    {"Key": self.key, "VersionId": "v2", "IsLatest": True},
                    {"Key": "landing/ignored.txt", "VersionId": "ignored"},
                ],
                "IsTruncated": True,
                "NextKeyMarker": self.key,
                "NextVersionIdMarker": "v2",
            }
        return {
            "Versions": [{"Key": self.key, "VersionId": "v1", "IsLatest": False}],
            "DeleteMarkers": [
                {"Key": self.key, "VersionId": "deleted", "IsLatest": False}
            ],
            "IsTruncated": False,
        }

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.get_requests.append(kwargs)
        metadata = {"sha256": self.checksum}
        if kwargs["VersionId"] == "v1":
            metadata = {"sha256": "damaged"}
        return {"Body": BytesIO(self.body), "Metadata": metadata}


def test_audit_s3_landing_versions_reads_history_and_delete_markers(
    tmp_path: Path, valid_source_payload: dict[str, Any]
) -> None:
    path = land_payload(tmp_path, valid_source_payload)
    document = json.loads(path.read_text(encoding="utf-8"))
    checksum = document["ingestion"]["object_checksum"]
    key = f"landing/{path.relative_to(tmp_path).as_posix()}"
    client = FakeS3VersionAuditClient(key, path.read_bytes(), checksum)

    report = audit_s3_landing_versions(client, bucket="lakehouse", prefix="/landing/")

    assert report.healthy is False
    assert report.valid == 1
    assert report.invalid == 1
    assert report.as_dict()["delete_marker_count"] == 1
    assert report.delete_markers[0].version_id == "deleted"
    assert report.items[0].version_id == "v1"
    assert report.items[1].version_id == "v2"
    assert client.list_requests[1]["KeyMarker"] == key
    assert client.list_requests[1]["VersionIdMarker"] == "v2"
    assert {request["VersionId"] for request in client.get_requests} == {"v1", "v2"}


def test_audit_s3_landing_versions_rejects_broken_pagination() -> None:
    class BrokenClient:
        def list_object_versions(self, **kwargs: Any) -> dict[str, Any]:
            return {"IsTruncated": True}

    with pytest.raises(ValueError, match="next key marker"):
        audit_s3_landing_versions(BrokenClient(), bucket="lakehouse")
