from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
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
        self.last_modified: dict[str, datetime] = {}

    def add_manifest(
        self, batch_id: str, batch_at: str, *, committed_at: datetime | None = None,
        tables: dict[str, Any] | None = None,
    ) -> None:
        if tables is None:
            tables = {
                name: {"file": f"{name}.jsonl", "rows": 1, "sha256": "0" * 64}
                for name in ("customers", "products", "orders", "payments")
            }
        body = (
            json.dumps(
                {
                    "batch_id": batch_id,
                    "config": {"batch_at": batch_at},
                    "tables": tables,
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
        if committed_at is not None:
            self.last_modified[key] = committed_at

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        keys = sorted(key for key in self.objects if key.startswith(kwargs["Prefix"]))
        return {
            "Contents": [
                {"Key": key, "LastModified": self.last_modified.get(key)}
                for key in keys
            ],
            "IsTruncated": False,
        }

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        body, metadata = self.objects[kwargs["Key"]]
        return {"Body": BytesIO(body), "Metadata": metadata}


@pytest.fixture
def client() -> FakeS3Client:
    result = FakeS3Client()
    result.add_manifest("bbbbbbbbbbbbbbbb", "2026-02-01T00:00:00Z")
    result.add_manifest("aaaaaaaaaaaaaaaa", "2026-01-01T00:00:00Z")
    return result


def test_source_freshness_uses_commit_time_not_event_time(tmp_path: Path) -> None:
    now = datetime(2026, 9, 27, 20, 0, tzinfo=UTC)
    client = FakeS3Client()
    client.add_manifest(
        "aaaaaaaaaaaaaaaa", "2026-01-01T00:00:00Z", committed_at=now - timedelta(seconds=30)
    )
    client.add_manifest(
        "bbbbbbbbbbbbbbbb", "2026-02-01T00:00:00Z", committed_at=now - timedelta(hours=1)
    )
    state = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)

    fresh = planner.check_source_freshness(max_age_seconds=60, now=now)
    stale = planner.check_source_freshness(max_age_seconds=20, now=now)

    assert fresh["status"] == "ready"
    assert fresh["latest_batch_id"] == "aaaaaaaaaaaaaaaa"
    assert fresh["latest_committed_at"] == "2026-09-27T19:59:30Z"
    assert fresh["age_seconds"] == 30
    assert stale["status"] == "stale"
    assert stale["reason"] == "age_limit_exceeded"
    assert not state.exists()

    just_over_limit = planner.check_source_freshness(
        max_age_seconds=30, now=now + timedelta(milliseconds=1)
    )
    assert just_over_limit["status"] == "stale"
    assert just_over_limit["age_seconds"] == 31


def test_source_freshness_fails_closed_without_commits_or_timestamp(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 9, 27, 20, 0, tzinfo=UTC)
    client = FakeS3Client()
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=tmp_path / "state.json")
    empty = planner.check_source_freshness(max_age_seconds=60, now=now)
    assert empty["status"] == "stale"
    assert empty["reason"] == "no_committed_batches"

    client.add_manifest("aaaaaaaaaaaaaaaa", "2026-01-01T00:00:00Z")
    with pytest.raises(CommerceBatchError, match="missing a committed manifest timestamp"):
        planner.check_source_freshness(max_age_seconds=60, now=now)
    with pytest.raises(CommerceBatchError, match="must be positive"):
        planner.check_source_freshness(max_age_seconds=0, now=now)


def test_backlog_freshness_tracks_oldest_pending_commit(tmp_path: Path) -> None:
    now = datetime(2026, 9, 27, 20, 0, tzinfo=UTC)
    client = FakeS3Client()
    client.add_manifest(
        "aaaaaaaaaaaaaaaa", "2026-01-01T00:00:00Z", committed_at=now - timedelta(seconds=20)
    )
    client.add_manifest(
        "bbbbbbbbbbbbbbbb", "2026-02-01T00:00:00Z", committed_at=now - timedelta(seconds=80)
    )
    state = tmp_path / "checkpoint.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)

    stale = planner.check_backlog_freshness(max_age_seconds=60, now=now)
    assert stale["status"] == "stale"
    assert stale["reason"] == "pending_age_limit_exceeded"
    assert stale["oldest_pending_batch_id"] == "bbbbbbbbbbbbbbbb"
    assert stale["pending_batches"] == 2
    assert stale["age_seconds"] == 80
    assert not state.exists()

    planner.commit("aaaaaaaaaaaaaaaa")
    assert planner.check_backlog_freshness(max_age_seconds=60, now=now)["pending_batches"] == 1
    planner.commit("bbbbbbbbbbbbbbbb")
    clear = planner.check_backlog_freshness(max_age_seconds=60, now=now)
    assert clear["status"] == "ready"
    assert clear["pending_batches"] == 0
    assert clear["age_seconds"] is None


def test_backlog_freshness_fails_closed_on_missing_timestamp_or_bad_checkpoint(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 9, 27, 20, 0, tzinfo=UTC)
    client = FakeS3Client()
    state = tmp_path / "checkpoint.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)
    assert planner.check_backlog_freshness(max_age_seconds=60, now=now)["status"] == "ready"

    client.add_manifest("aaaaaaaaaaaaaaaa", "2026-01-01T00:00:00Z")
    with pytest.raises(CommerceBatchError, match="missing a committed manifest timestamp"):
        planner.check_backlog_freshness(max_age_seconds=60, now=now)
    with pytest.raises(CommerceBatchError, match="must be positive"):
        planner.check_backlog_freshness(max_age_seconds=0, now=now)

    state.write_text("not json", encoding="utf-8")
    with pytest.raises(CommerceBatchError, match="checkpoint is unreadable"):
        planner.check_backlog_freshness(max_age_seconds=60, now=now)


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


def test_commit_rejects_a_manifest_changed_since_planning(
    client: FakeS3Client, tmp_path: Path,
) -> None:
    state = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)
    batch = planner.plan(max_batches=1)["batches"][0]
    client.add_manifest(batch["batch_id"], "2026-01-02T00:00:00Z")
    with pytest.raises(CommerceBatchError, match="changed during processing"):
        planner.commit(batch["batch_id"], expected_manifest_sha256=batch["manifest_sha256"])
    assert not state.exists()


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


def test_commit_cannot_skip_an_earlier_unprocessed_batch(
    client: FakeS3Client, tmp_path: Path
) -> None:
    state_path = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state_path)

    with pytest.raises(CommerceBatchError, match="earlier unprocessed batch"):
        planner.commit("bbbbbbbbbbbbbbbb")

    assert not state_path.exists()
    assert planner.commit("aaaaaaaaaaaaaaaa")["created"] is True
    assert planner.commit("bbbbbbbbbbbbbbbb")["created"] is True


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


@pytest.mark.parametrize(
    "damage", ["missing-table", "reused-file", "bad-checksum", "negative-rows", "bool-rows"]
)
def test_rejects_invalid_committed_table_inventory(
    tmp_path: Path, damage: str
) -> None:
    client = FakeS3Client()
    tables = {
        name: {"file": f"{name}.jsonl", "rows": 1, "sha256": "0" * 64}
        for name in ("customers", "products", "orders", "payments")
    }
    if damage == "missing-table":
        del tables["payments"]
    elif damage == "reused-file":
        tables["customers"]["file"] = "orders.jsonl"
    elif damage == "bad-checksum":
        tables["products"]["sha256"] = "not-a-checksum"
    elif damage == "negative-rows":
        tables["orders"]["rows"] = -1
    else:
        tables["orders"]["rows"] = True
    client.add_manifest("aaaaaaaaaaaaaaaa", "2026-01-01T00:00:00Z", tables=tables)
    state = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)

    with pytest.raises(CommerceBatchError, match="invalid commerce manifest"):
        planner.plan(max_batches=1)

    assert not state.exists()


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


@pytest.mark.parametrize("payload", ["[]", "null", "0", '"invalid"'])
def test_rejects_non_object_checkpoint_without_changing_it(
    client: FakeS3Client, tmp_path: Path, payload: str,
) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text(payload, encoding="utf-8")
    before = state_path.read_bytes()
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state_path)

    with pytest.raises(CommerceBatchError, match="unsupported structure"):
        planner.plan(max_batches=1)
    with pytest.raises(CommerceBatchError, match="unsupported structure"):
        planner.commit("aaaaaaaaaaaaaaaa")
    with pytest.raises(CommerceBatchError, match="unsupported structure"):
        planner.check_backlog_freshness(max_age_seconds=60)
    assert state_path.read_bytes() == before
