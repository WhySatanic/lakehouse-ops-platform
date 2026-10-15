from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from threading import Event, Lock, get_ident
from typing import Any

import pytest

import lakehouse_ops.ingestion.commerce_batches as batches_module
from lakehouse_ops.ingestion.commerce_batches import (
    MAX_COMMERCE_MANIFEST_BYTES,
    CommerceBatchError,
    CommerceBatchPlanner,
)

MISSING_SCHEMA = object()


def test_custom_client_stays_serial_unless_explicitly_opted_in(tmp_path):
    callers = []

    class SerialClient(FakeS3Client):
        def get_object(self, **kwargs):
            callers.append(get_ident())
            return super().get_object(**kwargs)

    client = SerialClient()
    for number in range(1, 17):
        client.add_manifest(f"{number:016x}", "2026-01-01T00:00:00Z")
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=tmp_path / "state.json")
    assert planner.plan(max_batches=1)["committed_batches"] == 16
    assert callers == [get_ident()] * 16


def test_invalid_parallel_group_does_not_enqueue_remaining_history(tmp_path):
    calls, bodies = [], []
    lock = Lock()

    class TrackingClient(FakeS3Client):
        def get_object(self, **kwargs):
            response = super().get_object(**kwargs)
            with lock:
                calls.append(kwargs["Key"])
                bodies.append(response["Body"])
            return response

    client = TrackingClient()
    for number in range(1, 25):
        client.add_manifest(f"{number:016x}", "2026-01-01T00:00:00Z")
    client.objects["landing/source=commerce/batch_id=0000000000000001/manifest.json"][1][
        "commit-marker"
    ] = "false"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=tmp_path / "state.json",
                                   manifest_read_workers=2)
    with pytest.raises(CommerceBatchError, match="invalid commerce commit marker"):
        planner.plan(max_batches=1)
    assert 1 <= len(calls) <= 2
    assert all(key.endswith(("0000000000000001/manifest.json", "0000000000000002/manifest.json"))
               for key in calls)
    assert all(body.closed for body in bodies)
    assert not (tmp_path / "state.json").exists()


@pytest.mark.parametrize("workers", [0, 9, True, 1.5])
def test_manifest_read_concurrency_is_bounded(workers, tmp_path):
    with pytest.raises(CommerceBatchError, match="manifest_read_workers"):
        CommerceBatchPlanner(FakeS3Client(), bucket="lakehouse",
                             state_path=tmp_path / "state.json", manifest_read_workers=workers)


def test_opted_in_manifest_reads_overlap_with_bounded_workers_and_sorted_output(tmp_path):
    ready, release = Event(), Event()
    lock = Lock()
    active = peak = calls = 0

    class BlockingClient(FakeS3Client):
        def get_object(self, **kwargs):
            nonlocal active, peak, calls
            with lock:
                active += 1
                calls += 1
                peak = max(peak, active)
                if active == 2:
                    ready.set()
            try:
                assert release.wait(3), "manifest readers did not overlap"
                return super().get_object(**kwargs)
            finally:
                with lock:
                    active -= 1

    client = BlockingClient()
    for number in range(24, 0, -1):
        client.add_manifest(f"{number:016x}", "2026-01-01T00:00:00Z")
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=tmp_path / "state.json",
                                   manifest_read_workers=2)
    with ThreadPoolExecutor(max_workers=1) as owner:
        future = owner.submit(planner.discover)
        try:
            assert ready.wait(2), "serial discovery did not overlap independent HTTP reads"
        finally:
            release.set()
        discovered = future.result(timeout=3)
    assert peak == 2 and calls == 24 and active == 0
    assert [batch.batch_id for batch in discovered] == [f"{n:016x}" for n in range(1, 25)]
    assert not (tmp_path / "state.json").exists()


@pytest.mark.parametrize("corruption", ["marker", "checksum", "body"])
def test_parallel_discovery_revalidates_all_manifests_and_closes_bodies(tmp_path, corruption):
    bodies = []
    lock = Lock()

    class TrackingClient(FakeS3Client):
        def get_object(self, **kwargs):
            response = super().get_object(**kwargs)
            with lock:
                bodies.append(response["Body"])
            return response

    client = TrackingClient()
    for number in range(1, 17):
        client.add_manifest(f"{number:016x}", "2026-01-01T00:00:00Z")
    state = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state,
                                   manifest_read_workers=4)
    assert planner.plan(max_batches=1)["committed_batches"] == 16
    assert all(body.closed for body in bodies)
    key = "landing/source=commerce/batch_id=0000000000000010/manifest.json"
    payload, metadata = client.objects[key]
    if corruption == "body":
        payload += b"changed"
    else:
        metadata = {**metadata, "commit-marker" if corruption == "marker" else "sha256": "bad"}
    client.objects[key] = payload, metadata
    bodies.clear()
    with pytest.raises(CommerceBatchError, match="invalid commerce commit marker"):
        planner.plan(max_batches=1)
    assert bodies and all(body.closed for body in bodies)
    assert not state.exists()


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}
        self.last_modified: dict[str, datetime] = {}

    def add_manifest(
        self, batch_id: str, batch_at: str, *, committed_at: datetime | None = None,
        tables: dict[str, Any] | None = None,
        schema_version: object = 1,
    ) -> None:
        if tables is None:
            tables = {
                name: {"file": f"{name}.jsonl", "rows": 1, "sha256": "0" * 64}
                for name in ("customers", "products", "orders", "payments")
            }
        manifest = {
            "batch_id": batch_id,
            "config": {"batch_at": batch_at},
            "tables": tables,
        }
        if schema_version is not MISSING_SCHEMA:
            manifest["schema_version"] = schema_version
        body = json.dumps(manifest, sort_keys=True).encode() + b"\n"
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


def test_discovery_reads_all_s3_listing_pages(tmp_path: Path) -> None:
    class PaginatedClient(FakeS3Client):
        def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
            if "ContinuationToken" not in kwargs:
                return {"Contents": [], "IsTruncated": True, "NextContinuationToken": "page-2"}
            assert kwargs["ContinuationToken"] == "page-2"
            return super().list_objects_v2(**kwargs)

    client = PaginatedClient()
    client.add_manifest("aaaaaaaaaaaaaaaa", "2026-01-01T00:00:00Z")
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=tmp_path / "state.json")

    assert [batch["batch_id"] for batch in planner.plan(max_batches=1)["batches"]] == [
        "aaaaaaaaaaaaaaaa"
    ]


@pytest.mark.parametrize("tokens", [("stuck", "stuck"), ("first", "second", "first")])
def test_discovery_rejects_repeated_s3_listing_token(
    tmp_path: Path, tokens: tuple[str, ...],
) -> None:
    class RepeatingPageClient(FakeS3Client):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
            self.calls += 1
            assert self.calls <= len(tokens), "planner requested another copy of the same page"
            return {
                "Contents": [],
                "IsTruncated": True,
                "NextContinuationToken": tokens[self.calls - 1],
            }

    client = RepeatingPageClient()
    state = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)

    with pytest.raises(CommerceBatchError, match="repeated continuation token"):
        planner.plan(max_batches=1)
    assert client.calls == len(tokens)
    assert not state.exists()


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


def test_concurrent_checkpoint_writer_cannot_acknowledge_the_same_batch_twice(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    client = FakeS3Client()
    client.add_manifest("aaaaaaaaaaaaaaaa", "2026-01-01T00:00:00Z")
    state_path = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state_path)
    entered, release = Event(), Event()
    original_write = batches_module._write_state
    writes = 0

    def pause_first_write(path: Path, state: dict[str, Any]) -> None:
        nonlocal writes
        writes += 1
        if writes == 1:
            entered.set()
            assert release.wait(timeout=5)
        original_write(path, state)

    monkeypatch.setattr(batches_module, "_write_state", pause_first_write)
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(planner.commit, "aaaaaaaaaaaaaaaa")
        assert entered.wait(timeout=5)
        try:
            with pytest.raises(CommerceBatchError, match="checkpoint is busy"):
                planner.commit("aaaaaaaaaaaaaaaa")
        finally:
            release.set()
        assert first.result(timeout=5)["created"] is True
    assert json.loads(state_path.read_text(encoding="utf-8"))["processed_batches"] == {
        "aaaaaaaaaaaaaaaa": {
            "batch_at": "2026-01-01T00:00:00Z",
            "manifest_sha256": client.objects[
                "landing/source=commerce/batch_id=aaaaaaaaaaaaaaaa/manifest.json"
            ][1]["sha256"],
        },
    }
    assert planner.commit("aaaaaaaaaaaaaaaa")["created"] is False


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


def test_active_batch_blocks_unrelated_commit_and_detects_changed_manifest(tmp_path):
    client = FakeS3Client()
    client.add_manifest("c" * 16, "2026-02-03T00:00:00Z")
    state = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)
    selected = planner.plan(max_batches=1)["batches"][0]
    planner.begin(selected)
    assert not state.exists()
    client.add_manifest("b" * 16, "2026-02-02T00:00:00Z")
    with pytest.raises(CommerceBatchError, match="cannot skip"):
        planner.commit("b" * 16)
    client.add_manifest("c" * 16, "2026-02-04T00:00:00Z")
    with pytest.raises(CommerceBatchError, match="active batch"):
        planner.plan(max_batches=1)
    with pytest.raises(CommerceBatchError, match="active batch"):
        planner.begin(selected)
    assert not state.exists()


def test_new_arrival_before_begin_is_replanned_without_starting_old_selection(tmp_path):
    client = FakeS3Client()
    client.add_manifest("c" * 16, "2026-02-03T00:00:00Z")
    state = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)
    selected = planner.plan(max_batches=1)["batches"][0]
    client.add_manifest("b" * 16, "2026-02-02T00:00:00Z")
    with pytest.raises(CommerceBatchError, match="no longer next"):
        planner.begin(selected)
    assert not state.exists()
    assert not Path(f"{state}.active.json").exists()
    assert planner.plan(max_batches=1)["batches"][0]["batch_id"] == "b" * 16


@pytest.mark.parametrize("content", ["{", "[]", '{"schema_version":1,"batch":[]}'])
def test_corrupt_active_batch_fails_closed(tmp_path, content):
    planner = CommerceBatchPlanner(FakeS3Client(), bucket="lakehouse",
                                   state_path=tmp_path / "state.json")
    (tmp_path / "state.json.active.json").write_text(content)
    with pytest.raises(CommerceBatchError, match="active batch"):
        planner.plan(max_batches=1)


def test_completed_checkpoint_with_leftover_intent_does_not_replay_compute(tmp_path):
    client = FakeS3Client()
    client.add_manifest("a" * 16, "2026-02-01T00:00:00Z")
    state = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)
    planner.begin(planner.plan(max_batches=1)["batches"][0])
    active = Path(f"{state}.active.json")
    intent = active.read_bytes()
    planner.commit("a" * 16)
    # Model a process exit between checkpoint replacement and intent cleanup.
    active.write_bytes(intent)
    assert planner.plan(max_batches=1)["batches"] == []
    client.add_manifest("b" * 16, "2026-02-02T00:00:00Z")
    planner.begin(planner.plan(max_batches=1)["batches"][0])
    assert json.loads(active.read_text())["batch"]["batch_id"] == "b" * 16


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


@pytest.mark.parametrize("extra_bytes", [0, 1, MAX_COMMERCE_MANIFEST_BYTES])
def test_manifest_read_limit_preserves_checkpoint(
    tmp_path: Path, extra_bytes: int
) -> None:
    class ShortReadBody(BytesIO):
        def __init__(self, payload: bytes) -> None:
            super().__init__(payload)
            self.bytes_read = 0

        def read(self, size: int = -1) -> bytes:
            assert 0 < size <= MAX_COMMERCE_MANIFEST_BYTES + 1
            chunk = super().read(min(size, 8192))
            self.bytes_read += len(chunk)
            assert self.bytes_read <= MAX_COMMERCE_MANIFEST_BYTES + 1
            return chunk

    class ShortReadClient(FakeS3Client):
        def get_object(self, **kwargs: Any) -> dict[str, Any]:
            body, metadata = self.objects[kwargs["Key"]]
            self.stream = ShortReadBody(body)
            return {"Body": self.stream, "Metadata": metadata}

    client = ShortReadClient()
    batch_id = "aaaaaaaaaaaaaaaa"
    client.add_manifest(batch_id, "2026-01-01T00:00:00Z")
    key = f"landing/source=commerce/batch_id={batch_id}/manifest.json"
    body, metadata = client.objects[key]
    body += b" " * (MAX_COMMERCE_MANIFEST_BYTES + extra_bytes - len(body))
    client.objects[key] = (body, {**metadata, "sha256": hashlib.sha256(body).hexdigest()})
    state = tmp_path / "state.json"
    state.write_text('{"schema_version":1,"processed_batches":{}}\n', encoding="utf-8")
    before = state.read_bytes()
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)

    if extra_bytes:
        with pytest.raises(CommerceBatchError, match="manifest exceeds"):
            planner.plan(max_batches=1)
    else:
        assert planner.plan(max_batches=1)["batches"][0]["batch_id"] == batch_id
    assert state.read_bytes() == before
    assert client.stream.bytes_read == min(len(body), MAX_COMMERCE_MANIFEST_BYTES + 1)
    assert client.stream.closed


@pytest.mark.parametrize("schema_version", [2, "1", True, None, MISSING_SCHEMA])
def test_rejects_unsupported_committed_schema_version(
    tmp_path: Path, schema_version: object
) -> None:
    client = FakeS3Client()
    client.add_manifest(
        "aaaaaaaaaaaaaaaa", "2026-01-01T00:00:00Z", schema_version=schema_version
    )
    state = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)

    with pytest.raises(CommerceBatchError, match="invalid commerce manifest"):
        planner.plan(max_batches=1)

    assert not state.exists()


@pytest.mark.parametrize("workers", [1, 2])
@pytest.mark.parametrize("batch_at", [
    None, True, 12, 1.5, [], {}, "not-a-timestamp", "2026-01-02T00:00:00",
    "0001-01-01T00:00:00+01:00", "9999-12-31T23:59:59-01:00",
])
def test_invalid_timestamp_is_a_batch_error_before_compute_and_preserves_state(
    tmp_path, batch_at, workers,
):
    from lakehouse_ops.commerce_pipeline import run_commerce_batch

    class TrackingClient(FakeS3Client):
        def get_object(self, **kwargs):
            response = super().get_object(**kwargs)
            streams.append(response["Body"])
            return response

    streams = []
    client = TrackingClient()
    client.add_manifest("aaaaaaaaaaaaaaaa", "2026-01-01T00:00:00Z")
    state = tmp_path / "checkpoint.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state,
                                   manifest_read_workers=workers)
    planner.commit("aaaaaaaaaaaaaaaa")
    before = state.read_bytes()
    client.add_manifest("bbbbbbbbbbbbbbbb", batch_at)
    streams.clear()

    def forbidden(*_):
        pytest.fail("invalid manifest must not compute or query gold")

    with pytest.raises(CommerceBatchError, match="invalid commerce manifest"):
        run_commerce_batch(planner, forbidden, run_stage=forbidden)
    assert state.read_bytes() == before
    assert not state.with_name("checkpoint.json.active.json").exists()
    assert streams and all(stream.closed for stream in streams)

    # Repair the rejected, never-started marker without clearing completed history.
    client.add_manifest("bbbbbbbbbbbbbbbb", "2026-01-02T00:00:00Z")
    selected = planner.plan(max_batches=1)["batches"]
    assert len(selected) == 1 and selected[0]["batch_id"] == "bbbbbbbbbbbbbbbb"
    assert selected[0]["batch_at"] == "2026-01-02T00:00:00Z"
    assert state.read_bytes() == before


@pytest.mark.parametrize(
    "damage",
    ["missing-table", "reused-file", "bad-checksum", "negative-rows", "zero-rows", "bool-rows"],
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
    elif damage == "zero-rows":
        tables["orders"]["rows"] = 0
    else:
        tables["orders"]["rows"] = True
    client.add_manifest("aaaaaaaaaaaaaaaa", "2026-01-01T00:00:00Z", tables=tables)
    state = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)

    with pytest.raises(CommerceBatchError, match="invalid commerce manifest"):
        planner.plan(max_batches=1)

    assert not state.exists()


@pytest.mark.parametrize("workers", [1, 8])
def test_rejects_changed_manifest_after_checkpoint(
    client: FakeS3Client, tmp_path: Path, workers: int,
) -> None:
    state_path = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state_path,
                                   manifest_read_workers=workers)
    planner.commit("aaaaaaaaaaaaaaaa")
    before = state_path.read_bytes()
    client.add_manifest("aaaaaaaaaaaaaaaa", "2026-01-02T00:00:00Z")

    with pytest.raises(CommerceBatchError, match="changed after processing"):
        planner.plan(max_batches=1)
    with pytest.raises(CommerceBatchError, match="changed after processing"):
        planner.commit("bbbbbbbbbbbbbbbb")
    assert state_path.read_bytes() == before


@pytest.mark.parametrize("workers", [1, 8])
def test_rejects_missing_processed_commit_marker(
    client: FakeS3Client, tmp_path: Path, workers: int,
) -> None:
    state_path = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state_path,
                                   manifest_read_workers=workers)
    planner.commit("aaaaaaaaaaaaaaaa")
    before = state_path.read_bytes()
    del client.objects[
        "landing/source=commerce/batch_id=aaaaaaaaaaaaaaaa/manifest.json"
    ]

    with pytest.raises(CommerceBatchError, match="commit marker is missing"):
        planner.plan(max_batches=1)
    assert state_path.read_bytes() == before


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


@pytest.mark.parametrize("workers", [1, 8])
@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("boolean_version", "unsupported structure"),
        ("missing_batch_at", "unsupported structure"),
        ("null_batch_at", "unsupported structure"),
        ("changed_batch_at", "checkpoint batch timestamp differs"),
    ],
)
def test_rejects_corrupt_processed_checkpoint_metadata_without_changing_it(
    client: FakeS3Client, tmp_path: Path, change: str, message: str, workers: int,
) -> None:
    state_path = tmp_path / "state.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state_path,
                                   manifest_read_workers=workers)
    planner.commit("aaaaaaaaaaaaaaaa")
    state = json.loads(state_path.read_text(encoding="utf-8"))
    processed = state["processed_batches"]["aaaaaaaaaaaaaaaa"]
    if change == "boolean_version":
        state["schema_version"] = True
    elif change == "missing_batch_at":
        del processed["batch_at"]
    elif change == "null_batch_at":
        processed["batch_at"] = None
    else:
        processed["batch_at"] = "2026-02-01T00:00:00Z"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    before = state_path.read_bytes()

    with pytest.raises(CommerceBatchError, match=message):
        planner.plan(max_batches=1)
    with pytest.raises(CommerceBatchError, match=message):
        planner.commit("bbbbbbbbbbbbbbbb")
    with pytest.raises(CommerceBatchError, match=message):
        planner.check_backlog_freshness(max_age_seconds=60)
    assert state_path.read_bytes() == before


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
