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
        assert forecast_days == 2
        return WeatherPayload.from_source(location, self.payload)


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]
        return {"ETag": '"test"'}


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


def test_ingest_weather_command_lands_payload_in_s3(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    valid_source_payload: dict[str, Any],
) -> None:
    FakeOpenMeteoClient.payload = valid_source_payload
    s3_client = FakeS3Client()
    monkeypatch.setattr(cli, "OpenMeteoClient", FakeOpenMeteoClient)
    monkeypatch.setattr(cli.boto3, "client", lambda *_, **__: s3_client)

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
            "--backend",
            "s3",
            "--s3-bucket",
            "lakehouse",
            "--s3-prefix",
            "landing",
            "--s3-endpoint-url",
            "http://localhost:9000",
        ]
    )

    result = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert result["created"] is True
    assert result["path"].startswith("s3://lakehouse/landing/")
    assert len(s3_client.objects) == 1


def test_ingest_weather_command_requires_bucket_for_s3(
    monkeypatch: pytest.MonkeyPatch,
    valid_source_payload: dict[str, Any],
) -> None:
    FakeOpenMeteoClient.payload = valid_source_payload
    monkeypatch.setattr(cli, "OpenMeteoClient", FakeOpenMeteoClient)

    with pytest.raises(SystemExit) as error:
        cli.main(
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
                "--backend",
                "s3",
                "--s3-bucket",
                "",
            ]
        )

    assert error.value.code == 2


def test_ingest_weather_batch_reports_every_location(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    valid_source_payload: dict[str, Any],
) -> None:
    FakeOpenMeteoClient.payload = valid_source_payload
    monkeypatch.setattr(cli, "OpenMeteoClient", FakeOpenMeteoClient)
    manifest = tmp_path / "locations.json"
    manifest.write_text(
        json.dumps(
            {
                "locations": [
                    {"name": "Moscow", "latitude": 55.7558, "longitude": 37.6173},
                    {"name": "Berlin", "latitude": 52.52, "longitude": 13.405},
                ]
            }
        ),
        encoding="utf-8",
    )

    exit_code = cli.main(
        [
            "ingest-weather-batch",
            "--locations",
            str(manifest),
            "--forecast-days",
            "2",
            "--max-workers",
            "2",
            "--output",
            str(tmp_path / "landing"),
        ]
    )

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert report["total"] == 2
    assert report["created"] == 2
    assert [item["location"] for item in report["items"]] == ["moscow", "berlin"]


def test_ingest_weather_batch_rejects_invalid_manifest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest = tmp_path / "locations.json"
    manifest.write_text("{}", encoding="utf-8")

    with pytest.raises(SystemExit) as error:
        cli.main(["ingest-weather-batch", "--locations", str(manifest)])

    assert error.value.code == 2
    assert "must contain a 'locations' array" in capsys.readouterr().err
