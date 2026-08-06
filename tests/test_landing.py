from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops.ingestion.landing import FileLandingZone
from lakehouse_ops.ingestion.models import Location, WeatherPayload


def test_landing_write_is_idempotent(
    tmp_path: Path, valid_source_payload: dict[str, Any]
) -> None:
    valid_source_payload["generationtime_ms"] = 0.12
    payload = WeatherPayload.from_source(
        Location("Moscow", 55.75, 37.62), valid_source_payload
    )
    landing = FileLandingZone(tmp_path)
    ingested_at = datetime(2026, 8, 6, 10, 30, tzinfo=UTC)

    first = landing.write(payload, ingested_at=ingested_at)
    repeated_source_payload = {**valid_source_payload, "generationtime_ms": 0.34}
    repeated_payload = WeatherPayload.from_source(payload.location, repeated_source_payload)
    second = landing.write(
        repeated_payload, ingested_at=datetime(2026, 8, 6, 11, 30, tzinfo=UTC)
    )

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
    payload = WeatherPayload.from_source(
        Location("moscow", 55.75, 37.62), valid_source_payload
    )

    with pytest.raises(ValueError, match="timezone-aware"):
        FileLandingZone(tmp_path).write(payload, ingested_at=datetime(2026, 8, 6))
