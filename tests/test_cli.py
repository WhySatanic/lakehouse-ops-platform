from __future__ import annotations

import json
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import pytest

from lakehouse_ops import cli
from lakehouse_ops.ingestion.models import Location, WeatherPayload


class FakeOpenMeteoClient:
    payload: dict[str, Any]

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def fetch(self, location: Location, *, forecast_days: int) -> WeatherPayload:
        assert location == Location("moscow", 55.7558, 37.6173)
        assert forecast_days == 2
        return WeatherPayload.from_source(location, self.payload)


def test_ingest_weather_command_lands_payload(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    valid_source_payload: dict[str, Any],
) -> None:
    FakeOpenMeteoClient.payload = valid_source_payload
    monkeypatch.setattr(cli, "OpenMeteoClient", FakeOpenMeteoClient)

    exit_code = cli.main(
        [
            "ingest-weather",
            "--name",
            "Moscow",
            "--latitude",
            "55.7558",
            "--longitude",
            "37.6173",
            "--forecast-days",
            "2",
            "--output",
            str(tmp_path),
        ]
    )

    result = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert result["created"] is True
    assert len(result["checksum"]) == 64
    assert Path(result["path"]).is_file()
