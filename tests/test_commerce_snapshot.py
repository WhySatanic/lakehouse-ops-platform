from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from lakehouse_ops.commerce_change_scenario import generate_change_scenario
from lakehouse_ops.commerce_snapshot import (
    SnapshotIdentity,
    load_snapshot_identity,
    snapshot_action,
)


def test_snapshot_manifest_binding_and_explicit_semantics(tmp_path: Path) -> None:
    batch = generate_change_scenario(tmp_path / "scenario")["batches"][0]
    root = Path(batch["path"])
    manifest = root / "manifest.json"
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    identity = load_snapshot_identity(root, batch_id=batch["batch_id"], manifest_sha256=digest)
    assert identity.batch_at == "2026-02-01T00:00:00+00:00"
    assert identity.manifest_sha256 == digest
    with pytest.raises(ValueError, match="requires the selected"):
        load_snapshot_identity(root, batch_id=batch["batch_id"], manifest_sha256="")
    with pytest.raises(ValueError, match="differs from the selected"):
        load_snapshot_identity(root, batch_id=batch["batch_id"], manifest_sha256="0" * 64)
    document = json.loads(manifest.read_text())
    document["config"].pop("source_semantics")
    manifest.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="explicitly declared"):
        load_snapshot_identity(
            root,
            batch_id=batch["batch_id"],
            manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
        )


def test_snapshot_ordering_replay_and_timestamp_conflicts() -> None:
    initial = SnapshotIdentity("a" * 16, "2026-02-01T00:00:00+00:00", "1" * 64)
    updated = SnapshotIdentity("b" * 16, "2026-02-02T00:00:00+00:00", "2" * 64)
    assert snapshot_action(None, initial) == "create"
    assert snapshot_action(initial, updated) == "replace"
    assert snapshot_action(updated, initial) == "stale"
    assert snapshot_action(updated, updated) == "replay"
    for conflict in (
        SnapshotIdentity("c" * 16, updated.batch_at, updated.manifest_sha256),
        SnapshotIdentity(updated.batch_id, updated.batch_at, "3" * 64),
    ):
        with pytest.raises(ValueError, match="conflicting snapshot"):
            snapshot_action(updated, conflict)
