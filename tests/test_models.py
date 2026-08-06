from __future__ import annotations

from typing import Any

import pytest

from lakehouse_ops.ingestion.models import Location, PayloadValidationError, WeatherPayload


def test_location_is_normalized() -> None:
    location = Location("New York", 40.7128, -74.006)

    assert location.name == "new-york"


@pytest.mark.parametrize(
    ("latitude", "longitude"),
    [(91, 0), (-91, 0), (0, 181), (0, -181)],
)
def test_location_rejects_invalid_coordinates(latitude: float, longitude: float) -> None:
    with pytest.raises(ValueError):
        Location("invalid", latitude, longitude)


def test_payload_rejects_series_with_different_length(
    valid_source_payload: dict[str, Any],
) -> None:
    valid_source_payload["hourly"]["temperature_2m"] = [18.1]

    with pytest.raises(PayloadValidationError, match="temperature_2m has 1 values; expected 2"):
        WeatherPayload.from_source(Location("moscow", 55.75, 37.62), valid_source_payload)


def test_payload_rejects_missing_series(valid_source_payload: dict[str, Any]) -> None:
    del valid_source_payload["hourly"]["precipitation"]

    with pytest.raises(PayloadValidationError, match="missing hourly series: precipitation"):
        WeatherPayload.from_source(Location("moscow", 55.75, 37.62), valid_source_payload)

