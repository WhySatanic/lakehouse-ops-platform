from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import boto3
import pytest

from lakehouse_ops.ingestion.models import Location, WeatherPayload
from lakehouse_ops.ingestion.s3_landing import S3LandingZone


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_S3_SMOKE") != "1", reason="requires an initialized local MinIO"
)
def test_real_minio_weather_replay(valid_source_payload: dict[str, Any]) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("LAKEOPS_S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "lakeops"),
        aws_secret_access_key=os.getenv(
            "MINIO_ROOT_PASSWORD", "lakeops-development-only"
        ),
        region_name="us-east-1",
    )
    landing = S3LandingZone(
        client,
        bucket=os.getenv("LAKEHOUSE_BUCKET", "lakehouse"),
        prefix=f"smoke/weather-replay-{uuid4().hex}",
    )
    payload = WeatherPayload.from_source(
        Location("Moscow", 55.75, 37.62), valid_source_payload
    )

    first = landing.write(payload, ingested_at=datetime(2026, 8, 6, 10, tzinfo=UTC))
    second = landing.write(payload, ingested_at=datetime(2026, 8, 6, 11, tzinfo=UTC))

    assert first.created is True
    assert second.created is False
    assert second.path == first.path
    assert second.checksum == first.checksum

    client.put_object(
        Bucket=os.getenv("LAKEHOUSE_BUCKET", "lakehouse"),
        Key=first.path.split("/", 3)[3],
        Body=b"{}",
        Metadata={"sha256": first.checksum},
    )
    with pytest.raises(ValueError, match="existing S3 object conflicts"):
        landing.write(payload, ingested_at=datetime(2026, 8, 6, 11, tzinfo=UTC))
