from __future__ import annotations

import hashlib
import json
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops.ingestion.commerce_batches import (
    CommerceBatchError,
    CommerceBatchPlanner,
)


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}

    def add_manifest(self, batch_id: str, batch_at: str) -> None:
        body = (
            json.dumps(
                {
                    "batch_id": batch_id,
                    "config": {"batch_at": batch_at},
                    "tables": {"orders": {"file": "orders.jsonl"}},
                },
                sort_keys=True,
            ).encode()
            + b"\n"
        )
        checksum = hashlib.sha256(body).hexdigest()
        key = f"landing/source=commerce/batch_id={batch_id}/manifest.json"
        self.objects[key] = (
            body,
            {
                "batch-id": batch_id,
                "commit-marker": "true",
                "sha256": checksum,
                "source": "commerce",
            },
        )

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        keys = sorted(key for key in self.objects if key.startswith(kwargs["Prefix"]))
        return {"Contents": [{"Key": key} for key in keys], "IsTruncated": False}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        body, metadata = self.objects[kwargs["Key"]]
        return {"Body": BytesIO(body), "Metadata": metadata}


@pytest.fixture
def client() -> FakeS3Client:
    result = FakeS3Client()
    result.add_manifest("bbbbbbbbbbbbbbbb", "2026-02-01T00:00:00Z")
    result.add_manifest("aaaaaaaaaaaaaaaa", "2026-01-01T00:00:00Z")
    return result


def test_plans_unprocessed_committed_batches_in_event_time_order(
    client: FakeS3Client, tmp_path: Path
) -> None:
    planner = CommerceBatchPlanner(
        client, bucket="lakehouse", state_path=tmp_path / "state.json"
    )

    report = planner.plan(max_batches=1)

    assert report["mode"] == "incremental"
    assert report["selected_batches"] == 1
    assert report["batches"][0]["batch_id"] == "aaaaaaaaaaaaaaaa"
    assert report["batches"][0]["path"].endswith("batch_id=aaaaaaaaaaaaaaaa/")


def test_commit_is_atomic_idempotent_and_advances_incremental_plan(
    client: FakeS3Client, tmp_path: Path
) -> None:
    state_path = tmp_path / "nested" / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state_path)

    first = planner.commit("aaaaaaaaaaaaaaaa")
    second = planner.commit("aaaaaaaaaaaaaaaa")
    plan = planner.plan(max_batches=2)

    assert first["created"] is True
    assert second["created"] is False
    assert json.loads(state_path.read_text())["schema_version"] == 1
    assert [batch["batch_id"] for batch in plan["batches"]] == ["bbbbbbbbbbbbbbbb"]


def test_explicit_replay_is_bounded_and_does_not_move_checkpoint(
    client: FakeS3Client, tmp_path: Path
) -> None:
    state_path = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state_path)
    planner.commit("aaaaaaaaaaaaaaaa")
    before = state_path.read_bytes()

    report = planner.plan(
        max_batches=1, replay_batches=("aaaaaaaaaaaaaaaa",)
    )

    assert report["mode"] == "replay"
    assert report["batches"][0]["batch_id"] == "aaaaaaaaaaaaaaaa"
    assert state_path.read_bytes() == before


def test_ignores_uncommitted_table_objects(tmp_path: Path) -> None:
    client = FakeS3Client()
    client.objects[
        "landing/source=commerce/batch_id=aaaaaaaaaaaaaaaa/orders.jsonl"
    ] = (b"{}\n", {})
    planner = CommerceBatchPlanner(
        client, bucket="lakehouse", state_path=tmp_path / "state.json"
    )

    report = planner.plan(max_batches=1)

    assert report["committed_batches"] == 0
    assert report["batches"] == []


def test_rejects_invalid_commit_marker(client: FakeS3Client, tmp_path: Path) -> None:
    key = "landing/source=commerce/batch_id=aaaaaaaaaaaaaaaa/manifest.json"
    body, metadata = client.objects[key]
    client.objects[key] = (body, {**metadata, "commit-marker": "false"})
    planner = CommerceBatchPlanner(
        client, bucket="lakehouse", state_path=tmp_path / "state.json"
    )

    with pytest.raises(CommerceBatchError, match="invalid commerce commit marker"):
        planner.plan(max_batches=1)


def test_rejects_changed_manifest_after_checkpoint(
    client: FakeS3Client, tmp_path: Path
) -> None:
    state_path = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state_path)
    planner.commit("aaaaaaaaaaaaaaaa")
    client.add_manifest("aaaaaaaaaaaaaaaa", "2026-01-02T00:00:00Z")

    with pytest.raises(CommerceBatchError, match="changed after processing"):
        planner.plan(max_batches=1)


def test_rejects_missing_processed_commit_marker(
    client: FakeS3Client, tmp_path: Path
) -> None:
    state_path = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state_path)
    planner.commit("aaaaaaaaaaaaaaaa")
    del client.objects[
        "landing/source=commerce/batch_id=aaaaaaaaaaaaaaaa/manifest.json"
    ]

    with pytest.raises(CommerceBatchError, match="commit marker is missing"):
        planner.plan(max_batches=1)


@pytest.mark.parametrize(
    ("max_batches", "replay_batches", "message"),
    [
        (0, (), "must be positive"),
        (1, ("aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb"), "exceeds max_batches"),
        (2, ("aaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaa"), "must be unique"),
        (1, ("cccccccccccccccc",), "is not committed"),
    ],
)
def test_rejects_unbounded_or_unknown_replay(
    client: FakeS3Client,
    tmp_path: Path,
    max_batches: int,
    replay_batches: tuple[str, ...],
    message: str,
) -> None:
    planner = CommerceBatchPlanner(
        client, bucket="lakehouse", state_path=tmp_path / "state.json"
    )

    with pytest.raises(CommerceBatchError, match=message):
        planner.plan(max_batches=max_batches, replay_batches=replay_batches)


def test_rejects_corrupt_checkpoint(client: FakeS3Client, tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text("not json", encoding="utf-8")
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state_path)

    with pytest.raises(CommerceBatchError, match="checkpoint is unreadable"):
        planner.plan(max_batches=1)
