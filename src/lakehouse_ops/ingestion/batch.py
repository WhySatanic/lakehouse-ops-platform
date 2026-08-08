from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from lakehouse_ops.ingestion.landing import LandingResult
from lakehouse_ops.ingestion.models import Location

MAX_LOCATIONS = 100


class LocationManifestError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class BatchItemResult:
    location: str
    status: str
    path: str | None = None
    checksum: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class BatchRunReport:
    items: tuple[BatchItemResult, ...]

    @property
    def succeeded(self) -> int:
        return sum(item.status != "failed" for item in self.items)

    @property
    def failed(self) -> int:
        return sum(item.status == "failed" for item in self.items)

    @property
    def created(self) -> int:
        return sum(item.status == "created" for item in self.items)

    @property
    def duplicates(self) -> int:
        return sum(item.status == "duplicate" for item in self.items)

    def as_dict(self) -> dict[str, Any]:
        return {
            "total": len(self.items),
            "succeeded": self.succeeded,
            "failed": self.failed,
            "created": self.created,
            "duplicates": self.duplicates,
            "items": [asdict(item) for item in self.items],
        }


def load_location_manifest(path: Path) -> list[Location]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise LocationManifestError(f"cannot read location manifest: {error}") from error
    except json.JSONDecodeError as error:
        raise LocationManifestError(
            f"location manifest is not valid JSON at line {error.lineno}, column {error.colno}"
        ) from error

    if not isinstance(document, dict) or not isinstance(document.get("locations"), list):
        raise LocationManifestError("location manifest must contain a 'locations' array")

    entries = document["locations"]
    if not entries:
        raise LocationManifestError("location manifest must contain at least one location")
    if len(entries) > MAX_LOCATIONS:
        raise LocationManifestError(f"location manifest cannot exceed {MAX_LOCATIONS} locations")

    locations: list[Location] = []
    names: set[str] = set()
    for index, entry in enumerate(entries):
        location = _parse_location(entry, index)
        if location.name in names:
            raise LocationManifestError(f"duplicate location name: {location.name}")
        locations.append(location)
        names.add(location.name)
    return locations


def run_batch(
    locations: Sequence[Location],
    worker: Callable[[Location], LandingResult],
    *,
    max_workers: int,
) -> BatchRunReport:
    if not locations:
        raise ValueError("locations must not be empty")
    if not 1 <= max_workers <= 16:
        raise ValueError("max_workers must be between 1 and 16")

    results: list[BatchItemResult | None] = [None] * len(locations)
    with ThreadPoolExecutor(max_workers=min(max_workers, len(locations))) as executor:
        futures = {
            executor.submit(worker, location): (index, location)
            for index, location in enumerate(locations)
        }
        for future in as_completed(futures):
            index, location = futures[future]
            try:
                landing = future.result()
                results[index] = BatchItemResult(
                    location=location.name,
                    status="created" if landing.created else "duplicate",
                    path=str(landing.path),
                    checksum=landing.checksum,
                )
            except Exception as error:
                results[index] = BatchItemResult(
                    location=location.name,
                    status="failed",
                    error=f"{type(error).__name__}: {error}",
                )

    return BatchRunReport(tuple(result for result in results if result is not None))


def _parse_location(entry: object, index: int) -> Location:
    if not isinstance(entry, dict):
        raise LocationManifestError(f"locations[{index}] must be an object")

    name = entry.get("name")
    latitude = entry.get("latitude")
    longitude = entry.get("longitude")
    if not isinstance(name, str):
        raise LocationManifestError(f"locations[{index}].name must be a string")
    if not _is_number(latitude):
        raise LocationManifestError(f"locations[{index}].latitude must be a number")
    if not _is_number(longitude):
        raise LocationManifestError(f"locations[{index}].longitude must be a number")

    try:
        return Location(name, float(latitude), float(longitude))
    except ValueError as error:
        raise LocationManifestError(f"locations[{index}]: {error}") from error


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
