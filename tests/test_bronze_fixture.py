from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakehouse_ops.ingestion.landing import prepare_landing_object
from lakehouse_ops.ingestion.models import Location, WeatherPayload


@pytest.mark.parametrize(
    "fixture_name",
    ["open_meteo_landing.json", "open_meteo_silver_cases.json"],
)
def test_bronze_fixture_is_a_canonical_landing_object(fixture_name: str) -> None:
    fixture_path = Path(__file__).parent / "fixtures" / fixture_name
    document = json.loads(fixture_path.read_text(encoding="utf-8"))
    payload = WeatherPayload.from_source(
        Location(
            document["ingestion"]["location"]["name"],
            document["ingestion"]["location"]["latitude"],
            document["ingestion"]["location"]["longitude"],
        ),
        document["payload"],
    )

    prepared = prepare_landing_object(
        payload,
        ingested_at=datetime.fromisoformat(document["ingestion"]["ingested_at"]).astimezone(
            UTC
        ),
    )

    assert json.loads(prepared.body) == document
    assert prepared.checksum == document["ingestion"]["object_checksum"]
