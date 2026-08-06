from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest
from botocore.exceptions import ClientError

from lakehouse_ops.ingestion.models import Location, WeatherPayload
from lakehouse_ops.ingestion.s3_landing import S3LandingZone


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
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
        return {"ETag": '"test"'}


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


def test_s3_landing_rejects_empty_bucket() -> None:
    with pytest.raises(ValueError, match="bucket must not be empty"):
        S3LandingZone(FakeS3Client(), bucket="")


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
