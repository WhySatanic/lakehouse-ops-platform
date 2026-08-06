from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class PayloadValidationError(ValueError):
    """Raised when a source response violates the expected data contract."""


@dataclass(frozen=True, slots=True)
class Location:
    name: str
    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        normalized_name = self.name.strip().lower().replace(" ", "-")
        if not normalized_name or not all(
            character.isalnum() or character in {"-", "_"} for character in normalized_name
        ):
            raise ValueError("location name must contain only letters, numbers, '-' or '_'")
        if not -90 <= self.latitude <= 90:
            raise ValueError("latitude must be between -90 and 90")
        if not -180 <= self.longitude <= 180:
            raise ValueError("longitude must be between -180 and 180")
        object.__setattr__(self, "name", normalized_name)


@dataclass(frozen=True, slots=True)
class WeatherPayload:
    location: Location
    source_payload: dict[str, Any]

    @classmethod
    def from_source(cls, location: Location, payload: dict[str, Any]) -> WeatherPayload:
        hourly = payload.get("hourly")
        if not isinstance(hourly, dict):
            raise PayloadValidationError("response field 'hourly' must be an object")

        times = hourly.get("time")
        if not isinstance(times, list) or not times:
            raise PayloadValidationError("response field 'hourly.time' must be a non-empty list")

        required_series = (
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
        )
        expected_length = len(times)
        for series_name in required_series:
            series = hourly.get(series_name)
            if not isinstance(series, list):
                raise PayloadValidationError(f"missing hourly series: {series_name}")
            if len(series) != expected_length:
                raise PayloadValidationError(
                    f"hourly series {series_name} has {len(series)} values; "
                    f"expected {expected_length}"
                )

        return cls(location=location, source_payload=payload)

