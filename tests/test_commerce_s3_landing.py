from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from lakehouse_ops.ingestion.commerce_fixture import (
    CommerceFixtureConfig,
    generate_commerce_fixture,
)
from lakehouse_ops.ingestion.commerce_s3_landing import (
    CommerceLandingError,
    CommerceS3LandingZone,
)


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, dict[str, str]]] = {}
        self.requests: list[dict[str, Any]] = []

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.requests.append(kwargs)
        object_id = (kwargs["Bucket"], kwargs["Key"])
        if object_id in self.objects:
            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed", "Message": "exists"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        self.objects[object_id] = (kwargs["Body"], kwargs["Metadata"])
        return {"ETag": '"test"'}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        body, metadata = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        return {"Body": BytesIO(body), "Metadata": metadata}


@pytest.fixture
def commerce_fixture(tmp_path: Path) -> Path:
    return generate_commerce_fixture(
        tmp_path,
        CommerceFixtureConfig(
            customers=4,
            products=2,
            orders=6,
            null_customer_emails=1,
            duplicate_orders=1,
            late_orders=1,
            invalid_payments=1,
        ),
    ).path


def test_lands_verified_tables_before_manifest_and_retries_safely(
    commerce_fixture: Path,
) -> None:
    client = FakeS3Client()
    landing = CommerceS3LandingZone(client, bucket="lakehouse", prefix="landing/training")

    first = landing.write(commerce_fixture)
    second = landing.write(commerce_fixture)

    assert first.created == 5
    assert second.created == 0
    assert first.objects == 5
    assert first.path == (
        f"s3://lakehouse/landing/training/source=commerce/batch_id={first.batch_id}/"
    )
    assert len(client.objects) == 5
    first_attempt_keys = [request["Key"] for request in client.requests[:5]]
    assert first_attempt_keys[-1].endswith("/manifest.json")
    assert all(request["IfNoneMatch"] == "*" for request in client.requests)
    manifest_request = client.requests[4]
    assert manifest_request["Metadata"]["commit-marker"] == "true"
    assert manifest_request["ContentType"] == "application/json"


def test_rejects_modified_fixture_before_upload(commerce_fixture: Path) -> None:
    (commerce_fixture / "orders.jsonl").write_text("tampered\n", encoding="utf-8")
    client = FakeS3Client()

    with pytest.raises(CommerceLandingError, match="checksum verification failed"):
        CommerceS3LandingZone(client, bucket="lakehouse").write(commerce_fixture)

    assert client.objects == {}


@pytest.mark.parametrize("document", [None, [], "invalid"])
def test_rejects_non_object_manifest_before_upload(
    commerce_fixture: Path, document: object
) -> None:
    (commerce_fixture / "manifest.json").write_text(json.dumps(document), encoding="utf-8")
    client = FakeS3Client()

    with pytest.raises(CommerceLandingError, match="fixture manifest is invalid"):
        CommerceS3LandingZone(client, bucket="lakehouse").write(commerce_fixture)

    assert client.requests == []


@pytest.mark.parametrize("batch_id", ["../escaped", "A" * 16])
def test_rejects_unaddressable_batch_id_before_upload(
    commerce_fixture: Path, batch_id: str
) -> None:
    manifest_path = commerce_fixture / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["batch_id"] = batch_id
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    client = FakeS3Client()

    with pytest.raises(CommerceLandingError, match="fixture batch_id is invalid"):
        CommerceS3LandingZone(client, bucket="lakehouse").write(commerce_fixture)

    assert client.requests == []


def test_rejects_conflicting_existing_object(commerce_fixture: Path) -> None:
    client = FakeS3Client()
    landing = CommerceS3LandingZone(client, bucket="lakehouse")
    first = landing.write(commerce_fixture)
    key = (
        "lakehouse",
        f"landing/source=commerce/batch_id={first.batch_id}/customers.jsonl",
    )
    body, metadata = client.objects[key]
    client.objects[key] = (body, {**metadata, "sha256": "0" * 64})

    with pytest.raises(CommerceLandingError, match="conflicts with fixture"):
        landing.write(commerce_fixture)


def test_rejects_empty_bucket() -> None:
    with pytest.raises(ValueError, match="bucket must not be empty"):
        CommerceS3LandingZone(FakeS3Client(), bucket="")
