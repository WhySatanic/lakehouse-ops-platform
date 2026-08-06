from __future__ import annotations

from typing import Any

import pytest


@pytest.fixture
def valid_source_payload() -> dict[str, Any]:
    return {
        "latitude": 55.75,
        "longitude": 37.62,
        "hourly": {
            "time": ["2026-08-06T00:00", "2026-08-06T01:00"],
            "temperature_2m": [18.1, 17.8],
            "relative_humidity_2m": [71, 73],
            "precipitation": [0.0, 0.1],
            "wind_speed_10m": [5.2, 4.9],
        },
    }

