from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

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


def audit_file_landing(root: Path) -> LandingAuditReport:
    resolved_root = root.resolve()
    if not root.is_dir():
        return LandingAuditReport(str(resolved_root), ())

    items = tuple(
        _audit_object(root, path) for path in sorted(root.rglob("*.json")) if path.is_file()
    )
    return LandingAuditReport(str(resolved_root), items)


def _audit_object(root: Path, path: Path) -> AuditItem:
    relative = path.relative_to(root)
    errors = _validate_layout(relative)

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
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

    errors.extend(_validate_document(relative, ingestion, source_payload))
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
    path: Path, ingestion: dict[str, Any], source_payload: dict[str, Any]
) -> list[str]:
    errors: list[str] = []
    if ingestion.get("source") != "open_meteo":
        errors.append("ingestion source must be open_meteo")

    location_data = ingestion.get("location")
    try:
        if not isinstance(location_data, dict):
            raise ValueError("ingestion location must be an object")
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

    if len(path.parts) == 4:
        if path.parts[2] != f"location={location.name}":
            errors.append("location partition does not match payload")
        _validate_ingestion_date(path.parts[1], ingestion.get("ingested_at"), errors)
    return errors


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
