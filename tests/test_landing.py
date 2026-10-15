from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops.ingestion.landing import FileLandingZone
from lakehouse_ops.ingestion.models import Location, WeatherPayload


def test_landing_write_is_idempotent(tmp_path: Path, valid_source_payload: dict[str, Any]) -> None:
    valid_source_payload["generationtime_ms"] = 0.12
    payload = WeatherPayload.from_source(Location("Moscow", 55.75, 37.62), valid_source_payload)
    landing = FileLandingZone(tmp_path)
    ingested_at = datetime(2026, 8, 6, 10, 30, tzinfo=UTC)

    first = landing.write(payload, ingested_at=ingested_at)
    repeated_source_payload = {**valid_source_payload, "generationtime_ms": 0.34}
    repeated_payload = WeatherPayload.from_source(payload.location, repeated_source_payload)
    second = landing.write(repeated_payload, ingested_at=datetime(2026, 8, 6, 11, 30, tzinfo=UTC))

    assert first.created is True
    assert second.created is False
    assert first.path == second.path
    assert first.checksum == second.checksum
    assert "ingest_date=2026-08-06" in first.path.as_posix()
    assert "location=moscow" in first.path.as_posix()
    landed_document = json.loads(first.path.read_text(encoding="utf-8"))
    assert landed_document["payload"] == valid_source_payload
    assert landed_document["ingestion"]["ingested_at"] == "2026-08-06T10:30:00+00:00"
    assert landed_document["ingestion"]["object_checksum"] == first.checksum


def test_landing_requires_timezone_aware_timestamp(
    tmp_path: Path, valid_source_payload: dict[str, Any]
) -> None:
    payload = WeatherPayload.from_source(Location("moscow", 55.75, 37.62), valid_source_payload)

    with pytest.raises(ValueError, match="timezone-aware"):
        FileLandingZone(tmp_path).write(payload, ingested_at=datetime(2026, 8, 6))


def test_landing_uses_utc_date_for_offset_timestamp(
    tmp_path: Path, valid_source_payload: dict[str, Any]
) -> None:
    payload = WeatherPayload.from_source(Location("moscow", 55.75, 37.62), valid_source_payload)
    local_time = datetime(2026, 8, 18, 1, 30, tzinfo=timezone(timedelta(hours=3)))

    result = FileLandingZone(tmp_path).write(payload, ingested_at=local_time)

    assert "ingest_date=2026-08-17" in result.path.as_posix()
    document = json.loads(result.path.read_text(encoding="utf-8"))
    assert document["ingestion"]["ingested_at"] == "2026-08-17T22:30:00+00:00"


@pytest.mark.parametrize("damage", ["json", "document", "checksum", "payload", "location", "date"])
def test_replay_refuses_corruption_and_preserves_existing_bytes(
    tmp_path: Path, valid_source_payload: dict[str, Any], damage: str,
) -> None:
    payload = WeatherPayload.from_source(Location("moscow", 55.75, 37.62), valid_source_payload)
    landing = FileLandingZone(tmp_path)
    at = datetime(2026, 8, 6, 10, 30, tzinfo=UTC)
    first = landing.write(payload, ingested_at=at)
    document = json.loads(first.path.read_bytes())
    if damage == "json":
        body = b"corrupt"
    elif damage == "document":
        body = b"[]"
    else:
        if damage == "checksum":
            document["ingestion"]["object_checksum"] = "0" * 64
        elif damage == "payload":
            document["payload"]["hourly"]["temperature_2m"][0] += 1
        elif damage == "location":
            document["ingestion"]["location"]["name"] = "another"
        else:
            document["ingestion"]["ingested_at"] = "2026-08-07T10:30:00+00:00"
        body = json.dumps(document).encode()
    first.path.write_bytes(body)

    with pytest.raises(ValueError, match="existing filesystem object conflicts"):
        landing.write(payload, ingested_at=at)

    assert first.path.read_bytes() == body
    assert not list(tmp_path.rglob("*.tmp"))


def test_replay_refuses_directory_at_object_path(
    tmp_path: Path, valid_source_payload: dict[str, Any],
) -> None:
    payload = WeatherPayload.from_source(Location("moscow", 55.75, 37.62), valid_source_payload)
    landing = FileLandingZone(tmp_path)
    at = datetime(2026, 8, 6, tzinfo=UTC)
    first = landing.write(payload, ingested_at=at)
    first.path.unlink()
    first.path.mkdir()

    with pytest.raises(ValueError, match="existing filesystem object is unreadable"):
        landing.write(payload, ingested_at=at)

    assert first.path.is_dir()


def test_replay_reports_unreadable_existing_object(
    tmp_path: Path, valid_source_payload: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = WeatherPayload.from_source(Location("moscow", 55.75, 37.62), valid_source_payload)
    landing = FileLandingZone(tmp_path)
    at = datetime(2026, 8, 6, tzinfo=UTC)
    first = landing.write(payload, ingested_at=at)
    original = Path.read_bytes

    def denied(path: Path) -> bytes:
        if path == first.path:
            raise PermissionError("read denied")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", denied)
    with pytest.raises(ValueError, match="existing filesystem object is unreadable") as caught:
        landing.write(payload, ingested_at=at)

    assert isinstance(caught.value.__cause__, PermissionError)
    assert original(first.path)
