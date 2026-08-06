from __future__ import annotations

from typing import Any

import httpx
import pytest

from lakehouse_ops.ingestion.models import Location
from lakehouse_ops.ingestion.open_meteo import OpenMeteoClient, OpenMeteoError


def test_fetch_retries_transient_response(valid_source_payload: dict[str, Any]) -> None:
    attempts = 0
    delays: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(503, request=request)
        return httpx.Response(200, json=valid_source_payload, request=request)

    with OpenMeteoClient(
        transport=httpx.MockTransport(handler), sleep=delays.append
    ) as client:
        payload = client.fetch(Location("moscow", 55.75, 37.62))

    assert payload.source_payload == valid_source_payload
    assert attempts == 2
    assert delays == [0.25]


def test_fetch_does_not_retry_client_error() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(400, request=request)

    with (
        OpenMeteoClient(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(OpenMeteoError) as error,
    ):
        client.fetch(Location("moscow", 55.75, 37.62))

    assert attempts == 1
    assert isinstance(error.value.__cause__, httpx.HTTPStatusError)


@pytest.mark.parametrize("forecast_days", [0, 17])
def test_fetch_rejects_unbounded_forecast(forecast_days: int) -> None:
    with (
        OpenMeteoClient(transport=httpx.MockTransport(lambda _: httpx.Response(200))) as client,
        pytest.raises(ValueError, match="between 1 and 16"),
    ):
        client.fetch(Location("moscow", 55.75, 37.62), forecast_days=forecast_days)
