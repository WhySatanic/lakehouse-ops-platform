from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakehouse_ops.ingestion.batch import (
    LocationManifestError,
    load_location_manifest,
    run_batch,
)
from lakehouse_ops.ingestion.landing import LandingResult
from lakehouse_ops.ingestion.models import Location


def write_manifest(path: Path, document: object) -> None:
    path.write_text(json.dumps(document), encoding="utf-8")


def test_load_location_manifest_normalizes_names(tmp_path: Path) -> None:
    path = tmp_path / "locations.json"
    write_manifest(
        path,
        {
            "locations": [
                {"name": "New York", "latitude": 40.7128, "longitude": -74.006},
                {"name": "Berlin", "latitude": 52.52, "longitude": 13.405},
            ]
        },
    )

    assert load_location_manifest(path) == [
        Location("new-york", 40.7128, -74.006),
        Location("berlin", 52.52, 13.405),
    ]


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({}, "must contain a 'locations' array"),
        ({"locations": []}, "at least one location"),
        (
            {
                "locations": [
                    {"name": "Moscow", "latitude": 55.7, "longitude": 37.6},
                    {"name": "moscow", "latitude": 55.8, "longitude": 37.7},
                ]
            },
            "duplicate location name: moscow",
        ),
        (
            {"locations": [{"name": "north", "latitude": 91, "longitude": 0}]},
            "latitude must be between -90 and 90",
        ),
    ],
)
def test_load_location_manifest_rejects_invalid_documents(
    tmp_path: Path, document: object, message: str
) -> None:
    path = tmp_path / "locations.json"
    write_manifest(path, document)

    with pytest.raises(LocationManifestError, match=message):
        load_location_manifest(path)


def test_run_batch_preserves_order_and_captures_partial_failures() -> None:
    locations = [
        Location("first", 1, 1),
        Location("second", 2, 2),
        Location("third", 3, 3),
    ]

    def worker(location: Location) -> LandingResult:
        if location.name == "second":
            raise RuntimeError("source unavailable")
        return LandingResult(
            path=f"landing/{location.name}.json",
            checksum=location.name * 8,
            created=location.name == "first",
        )

    report = run_batch(locations, worker, max_workers=2)

    assert [item.location for item in report.items] == ["first", "second", "third"]
    assert [item.status for item in report.items] == ["created", "failed", "duplicate"]
    assert report.as_dict() | {"items": []} == {
        "total": 3,
        "succeeded": 2,
        "failed": 1,
        "created": 1,
        "duplicates": 1,
        "items": [],
    }
