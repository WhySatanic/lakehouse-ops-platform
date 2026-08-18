from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from lakehouse_ops.ingestion.models import WeatherPayload


@dataclass(frozen=True, slots=True)
class LandingResult:
    path: Path | str
    checksum: str
    created: bool


@dataclass(frozen=True, slots=True)
class LandingObject:
    key: str
    checksum: str
    body: bytes


def prepare_landing_object(
    payload: WeatherPayload, *, ingested_at: datetime | None = None
) -> LandingObject:
    timestamp = ingested_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        raise ValueError("ingested_at must be timezone-aware")
    timestamp = timestamp.astimezone(UTC)

    checksum = calculate_object_checksum(payload)
    ingestion_identity = {
        "source": "open_meteo",
        "location": {
            "name": payload.location.name,
            "latitude": payload.location.latitude,
            "longitude": payload.location.longitude,
        },
    }
    key = (
        "source=open_meteo/"
        f"ingest_date={timestamp.date().isoformat()}/"
        f"location={payload.location.name}/"
        f"{checksum}.json"
    )
    document = {
        "ingestion": {
            "source": ingestion_identity["source"],
            "ingested_at": timestamp.isoformat(),
            "location": ingestion_identity["location"],
            "object_checksum": checksum,
        },
        "payload": payload.source_payload,
    }
    body = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return LandingObject(key=key, checksum=checksum, body=body)


def calculate_object_checksum(payload: WeatherPayload) -> str:
    stable_source_payload = {
        key: value for key, value in payload.source_payload.items() if key != "generationtime_ms"
    }
    object_identity = {
        "source": "open_meteo",
        "location": {
            "name": payload.location.name,
            "latitude": payload.location.latitude,
            "longitude": payload.location.longitude,
        },
        "payload": stable_source_payload,
    }
    canonical_identity = json.dumps(
        object_identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical_identity).hexdigest()


class FileLandingZone:
    """Filesystem adapter mirroring the future object-store key layout."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def write(
        self, payload: WeatherPayload, *, ingested_at: datetime | None = None
    ) -> LandingResult:
        landing_object = prepare_landing_object(payload, ingested_at=ingested_at)
        destination = self._root / Path(landing_object.key)
        destination.parent.mkdir(parents=True, exist_ok=True)

        if destination.exists():
            return LandingResult(path=destination, checksum=landing_object.checksum, created=False)

        descriptor, temporary_name = tempfile.mkstemp(
            dir=destination.parent, prefix=f".{landing_object.checksum}.", suffix=".tmp"
        )
        try:
            with os.fdopen(descriptor, "wb") as temporary_file:
                temporary_file.write(landing_object.body)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            os.replace(temporary_name, destination)
        finally:
            if os.path.exists(temporary_name):
                os.unlink(temporary_name)

        return LandingResult(path=destination, checksum=landing_object.checksum, created=True)
