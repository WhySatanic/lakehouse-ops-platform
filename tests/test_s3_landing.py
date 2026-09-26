from __future__ import annotations

import json
from datetime import UTC, datetime
from io import BytesIO
from typing import Any

import pytest
from botocore.exceptions import ClientError

from lakehouse_ops.ingestion.models import Location, WeatherPayload
from lakehouse_ops.ingestion.s3_landing import S3LandingZone


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.metadata: dict[tuple[str, str], dict[str, str]] = {}
        self.last_request: dict[str, Any] = {}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.last_request = kwargs
        object_id = (kwargs["Bucket"], kwargs["Key"])
        if object_id in self.objects:
            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed", "Message": "exists"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        self.objects[object_id] = kwargs["Body"]
        self.metadata[object_id] = kwargs["Metadata"]
        return {"ETag": '"test"'}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        object_id = (kwargs["Bucket"], kwargs["Key"])
        return {"Body": BytesIO(self.objects[object_id]), "Metadata": self.metadata[object_id]}


def test_s3_landing_uses_conditional_idempotent_write(
    valid_source_payload: dict[str, Any],
) -> None:
    client = FakeS3Client()
    payload = WeatherPayload.from_source(
        Location("Moscow", 55.75, 37.62), valid_source_payload
    )
    landing = S3LandingZone(client, bucket="lakehouse", prefix="landing")
    ingested_at = datetime(2026, 8, 6, 10, 30, tzinfo=UTC)

    first = landing.write(payload, ingested_at=ingested_at)
    second = landing.write(payload, ingested_at=ingested_at)

    assert first.created is True
    assert second.created is False
    assert first == second.__class__(
        path=second.path, checksum=second.checksum, created=True
    )
    assert first.path.startswith("s3://lakehouse/landing/source=open_meteo/")
    assert client.last_request["IfNoneMatch"] == "*"
    assert client.last_request["ContentType"] == "application/json"
    assert client.last_request["Metadata"]["sha256"] == first.checksum
    stored = next(iter(client.objects.values()))
    assert json.loads(stored)["ingestion"]["object_checksum"] == first.checksum


def test_s3_landing_replay_accepts_new_ingestion_timestamp(
    valid_source_payload: dict[str, Any],
) -> None:
    client = FakeS3Client()
    payload = WeatherPayload.from_source(
        Location("Moscow", 55.75, 37.62), valid_source_payload
    )
    landing = S3LandingZone(client, bucket="lakehouse", prefix="landing")

    first = landing.write(payload, ingested_at=datetime(2026, 8, 6, 10, tzinfo=UTC))
    second = landing.write(payload, ingested_at=datetime(2026, 8, 6, 11, tzinfo=UTC))

    assert first.checksum == second.checksum
    assert second.created is False


def test_s3_landing_rejects_empty_bucket() -> None:
    with pytest.raises(ValueError, match="bucket must not be empty"):
        S3LandingZone(FakeS3Client(), bucket="")


@pytest.mark.parametrize("corruption", ["body", "metadata"])
def test_s3_landing_rejects_conflicting_existing_object(
    valid_source_payload: dict[str, Any], corruption: str
) -> None:
    client = FakeS3Client()
    payload = WeatherPayload.from_source(
        Location("Moscow", 55.75, 37.62), valid_source_payload
    )
    landing = S3LandingZone(client, bucket="lakehouse", prefix="landing")
    landed = landing.write(payload, ingested_at=datetime(2026, 8, 6, tzinfo=UTC))
    object_id = ("lakehouse", landed.path.removeprefix("s3://lakehouse/"))

    if corruption == "metadata":
        client.metadata[object_id] = {"sha256": "0" * 64}
    else:
        document = json.loads(client.objects[object_id])
        document["payload"]["hourly"]["temperature_2m"][0] = 999
        client.objects[object_id] = json.dumps(document).encode()

    with pytest.raises(ValueError, match="existing S3 object conflicts"):
        landing.write(payload, ingested_at=datetime(2026, 8, 6, tzinfo=UTC))


def test_s3_landing_propagates_unexpected_client_error(
    valid_source_payload: dict[str, Any],
) -> None:
    class FailingClient:
        def put_object(self, **kwargs: Any) -> dict[str, Any]:
            raise ClientError(
                {
                    "Error": {"Code": "AccessDenied", "Message": "denied"},
                    "ResponseMetadata": {"HTTPStatusCode": 403},
                },
                "PutObject",
            )

    payload = WeatherPayload.from_source(
        Location("Moscow", 55.75, 37.62), valid_source_payload
    )

    with pytest.raises(ClientError):
        S3LandingZone(FailingClient(), bucket="lakehouse").write(payload)
