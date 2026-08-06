from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import httpx

from lakehouse_ops.ingestion.models import Location, WeatherPayload

DEFAULT_BASE_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
)
TRANSIENT_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


class OpenMeteoError(RuntimeError):
    pass


class OpenMeteoClient:
    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = 10.0,
        max_attempts: int = 3,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self._base_url = base_url
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._client = httpx.Client(timeout=timeout_seconds, transport=transport)

    def __enter__(self) -> OpenMeteoClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def fetch(self, location: Location, *, forecast_days: int = 3) -> WeatherPayload:
        if not 1 <= forecast_days <= 16:
            raise ValueError("forecast_days must be between 1 and 16")

        params: dict[str, Any] = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "hourly": ",".join(HOURLY_VARIABLES),
            "forecast_days": forecast_days,
            "timezone": "UTC",
        }
        last_error: Exception | None = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._client.get(self._base_url, params=params)
                if response.status_code in TRANSIENT_STATUS_CODES:
                    raise OpenMeteoError(f"transient source response: HTTP {response.status_code}")
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise OpenMeteoError("source response must be a JSON object")
                return WeatherPayload.from_source(location, payload)
            except (httpx.HTTPError, ValueError, OpenMeteoError) as error:
                last_error = error
                if attempt == self._max_attempts or not self._is_retryable(error):
                    break
                self._sleep(0.25 * (2 ** (attempt - 1)))

        raise OpenMeteoError(
            f"failed to fetch weather data after {self._max_attempts} attempt(s)"
        ) from last_error

    @staticmethod
    def _is_retryable(error: Exception) -> bool:
        return isinstance(error, (httpx.TransportError, OpenMeteoError))
