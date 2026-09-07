from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import ClientError

from lakehouse_ops.ingestion.landing import calculate_object_checksum
from lakehouse_ops.ingestion.models import Location, WeatherPayload


@dataclass(frozen=True, slots=True)
class AuditItem:
    path: str
    status: str
    errors: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class LandingAuditReport:
    root: str
    items: tuple[AuditItem, ...]

    @property
    def valid(self) -> int:
        return sum(item.status == "valid" for item in self.items)

    @property
    def invalid(self) -> int:
        return sum(item.status == "invalid" for item in self.items)

    @property
    def healthy(self) -> bool:
        return bool(self.items) and self.invalid == 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self.healthy else "failed",
            "root": self.root,
            "total": len(self.items),
            "valid": self.valid,
            "invalid": self.invalid,
            "items": [asdict(item) for item in self.items],
        }


class S3AuditClient(Protocol):
    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


def audit_file_landing(root: Path) -> LandingAuditReport:
    resolved_root = root.resolve()
    if not root.is_dir():
        return LandingAuditReport(str(resolved_root), ())

    items = tuple(
        _audit_object(root, path) for path in sorted(root.rglob("*.json")) if path.is_file()
    )
    return LandingAuditReport(str(resolved_root), items)


def audit_s3_landing(
    client: S3AuditClient, *, bucket: str, prefix: str = ""
) -> LandingAuditReport:
    if not bucket:
        raise ValueError("bucket must not be empty")

    normalized_prefix = prefix.strip("/")
    keys = _list_s3_json_keys(client, bucket=bucket, prefix=normalized_prefix)
    items = tuple(
        _audit_s3_object(
            client,
            bucket=bucket,
            key=key,
            relative_key=_relative_s3_key(key, normalized_prefix),
        )
        for key in keys
    )
    root = f"s3://{bucket}/{normalized_prefix}" if normalized_prefix else f"s3://{bucket}"
    return LandingAuditReport(root, items)


def _audit_object(root: Path, path: Path) -> AuditItem:
    relative = path.relative_to(root)

    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        errors = _validate_layout(relative)
        errors.append(f"cannot read JSON: {error}")
        return AuditItem(relative.as_posix(), "invalid", tuple(errors))

    return _audit_content(relative, content)


def _audit_s3_object(
    client: S3AuditClient, *, bucket: str, key: str, relative_key: str
) -> AuditItem:
    relative = Path(relative_key)
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
    except (ClientError, KeyError, OSError, UnicodeError) as error:
        errors = _validate_layout(relative)
        errors.append(f"cannot read object: {error}")
        return AuditItem(relative_key, "invalid", tuple(errors))

    metadata = response.get("Metadata")
    metadata_checksum = metadata.get("sha256") if isinstance(metadata, dict) else None
    item = _audit_content(relative, body, metadata_checksum=metadata_checksum)
    if metadata_checksum is not None:
        return item
    return AuditItem(
        path=item.path,
        status="invalid",
        errors=(*item.errors, "object metadata checksum is missing"),
    )


def _audit_content(
    relative: Path, content: str | bytes, *, metadata_checksum: object | None = None
) -> AuditItem:
    errors = _validate_layout(relative)

    try:
        document = json.loads(content)
    except (UnicodeError, json.JSONDecodeError) as error:
        errors.append(f"cannot read JSON: {error}")
        return AuditItem(relative.as_posix(), "invalid", tuple(errors))

    if not isinstance(document, dict):
        errors.append("document must be a JSON object")
        return AuditItem(relative.as_posix(), "invalid", tuple(errors))

    ingestion = document.get("ingestion")
    source_payload = document.get("payload")
    if not isinstance(ingestion, dict):
        errors.append("ingestion must be an object")
    if not isinstance(source_payload, dict):
        errors.append("payload must be an object")
    if errors and (not isinstance(ingestion, dict) or not isinstance(source_payload, dict)):
        return AuditItem(relative.as_posix(), "invalid", tuple(errors))

    errors.extend(
        _validate_document(
            relative,
            ingestion,
            source_payload,
            metadata_checksum=metadata_checksum,
        )
    )
    status = "valid" if not errors else "invalid"
    return AuditItem(relative.as_posix(), status, tuple(errors))


def _validate_layout(path: Path) -> list[str]:
    parts = path.parts
    if len(parts) != 4:
        return ["path must match source/date/location/checksum.json layout"]

    errors: list[str] = []
    if parts[0] != "source=open_meteo":
        errors.append("path source must be open_meteo")
    if not parts[1].startswith("ingest_date="):
        errors.append("path must contain ingest_date partition")
    if not parts[2].startswith("location="):
        errors.append("path must contain location partition")
    return errors


def _validate_document(
    path: Path,
    ingestion: dict[str, Any],
    source_payload: dict[str, Any],
    *,
    metadata_checksum: object | None = None,
) -> list[str]:
    errors: list[str] = []
    if ingestion.get("source") != "open_meteo":
        errors.append("ingestion source must be open_meteo")

    location_data = ingestion.get("location")
    try:
        if not isinstance(location_data, dict):
            raise ValueError("ingestion location must be an object")
        if not isinstance(location_data.get("name"), str):
            raise ValueError("ingestion location name must be a string")
        location = Location(
            location_data["name"],
            location_data["latitude"],
            location_data["longitude"],
        )
        payload = WeatherPayload.from_source(location, source_payload)
    except (KeyError, TypeError, ValueError) as error:
        errors.append(f"invalid weather payload: {error}")
        return errors

    expected_checksum = calculate_object_checksum(payload)
    declared_checksum = ingestion.get("object_checksum")
    if declared_checksum != expected_checksum:
        errors.append("declared checksum does not match payload")
    if path.stem != expected_checksum:
        errors.append("filename checksum does not match payload")
    if metadata_checksum is not None and metadata_checksum != expected_checksum:
        errors.append("object metadata checksum does not match payload")

    if len(path.parts) == 4:
        if path.parts[2] != f"location={location.name}":
            errors.append("location partition does not match payload")
        _validate_ingestion_date(path.parts[1], ingestion.get("ingested_at"), errors)
    return errors


def _list_s3_json_keys(
    client: S3AuditClient, *, bucket: str, prefix: str
) -> tuple[str, ...]:
    request: dict[str, Any] = {"Bucket": bucket}
    if prefix:
        request["Prefix"] = f"{prefix}/"

    keys: list[str] = []
    while True:
        response = client.list_objects_v2(**request)
        contents = response.get("Contents", ())
        if isinstance(contents, list):
            keys.extend(
                key
                for item in contents
                if isinstance(item, dict)
                and isinstance((key := item.get("Key")), str)
                and key.endswith(".json")
            )
        if not response.get("IsTruncated"):
            return tuple(sorted(keys))
        token = response.get("NextContinuationToken")
        if not isinstance(token, str) or not token:
            raise ValueError("truncated S3 listing has no continuation token")
        request["ContinuationToken"] = token


def _relative_s3_key(key: str, prefix: str) -> str:
    if not prefix:
        return key
    return key.removeprefix(f"{prefix}/")


def _validate_ingestion_date(partition: str, ingested_at: object, errors: list[str]) -> None:
    try:
        timestamp = datetime.fromisoformat(str(ingested_at))
        if timestamp.tzinfo is None:
            raise ValueError("timestamp has no timezone")
    except ValueError as error:
        errors.append(f"invalid ingested_at: {error}")
        return

    if partition != f"ingest_date={timestamp.date().isoformat()}":
        errors.append("ingest_date partition does not match ingested_at")
