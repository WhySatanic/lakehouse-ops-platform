from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import ClientError

from lakehouse_ops.ingestion.audit import audit_landing_content
from lakehouse_ops.ingestion.landing import LandingResult, prepare_landing_object
from lakehouse_ops.ingestion.models import WeatherPayload


class S3Client(Protocol):
    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


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
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            response_body = response["Body"]
            try:
                existing_body = response_body.read()
            finally:
                response_body.close()
            metadata = response.get("Metadata")
            existing_checksum = metadata.get("sha256") if isinstance(metadata, dict) else None
            item = audit_landing_content(
                Path(landing_object.key), existing_body, metadata_checksum=existing_checksum
            )
            if existing_checksum != landing_object.checksum or item.status != "valid":
                raise ValueError(f"existing S3 object conflicts with forecast: {key}") from None
            created = False

        return LandingResult(
            path=f"s3://{self._bucket}/{key}",
            checksum=landing_object.checksum,
            created=created,
        )
