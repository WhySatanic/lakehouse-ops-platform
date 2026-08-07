from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from botocore.exceptions import ClientError

from lakehouse_ops.ingestion.landing import LandingResult, prepare_landing_object
from lakehouse_ops.ingestion.models import WeatherPayload


class S3Client(Protocol):
    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...


class S3LandingZone:
    def __init__(self, client: S3Client, *, bucket: str, prefix: str = "") -> None:
        if not bucket:
            raise ValueError("bucket must not be empty")
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def write(
        self, payload: WeatherPayload, *, ingested_at: datetime | None = None
    ) -> LandingResult:
        landing_object = prepare_landing_object(payload, ingested_at=ingested_at)
        key = "/".join(part for part in (self._prefix, landing_object.key) if part)

        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=landing_object.body,
                ContentType="application/json",
                Metadata={
                    "sha256": landing_object.checksum,
                    "source": "open_meteo",
                },
                IfNoneMatch="*",
            )
            created = True
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = error.response.get("Error", {}).get("Code")
            if status != 412 and code not in {"PreconditionFailed", "412"}:
                raise
            created = False

        return LandingResult(
            path=f"s3://{self._bucket}/{key}",
            checksum=landing_object.checksum,
            created=created,
        )
