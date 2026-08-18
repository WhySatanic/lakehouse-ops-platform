from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lakehouse_ops.ingestion.audit import audit_file_landing
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
