"""Identity and ordering for explicitly declared complete commerce snapshots."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from lakehouse_ops.ingestion.commerce_bronze import load_commerce_batch


@dataclass(frozen=True, slots=True)
class SnapshotIdentity:
    batch_id: str
    batch_at: str
    manifest_sha256: str


def load_snapshot_identity(
    path: Path,
    *,
    batch_id: str,
    manifest_sha256: str,
) -> SnapshotIdentity:
    if not manifest_sha256:
        raise ValueError("snapshot requires the selected manifest SHA-256")
    batch = load_commerce_batch(
        path,
        expected_batch_id=batch_id,
        expected_manifest_sha256=manifest_sha256,
    )
    manifest_body = (path / "manifest.json").read_bytes()
    if hashlib.sha256(manifest_body).hexdigest() != manifest_sha256:
        raise ValueError("snapshot manifest changed during identity validation")
    manifest = json.loads(manifest_body)
    if manifest["config"].get("source_semantics") != "full_snapshot":
        raise ValueError("current mart requires explicitly declared full_snapshot semantics")
    at = datetime.fromisoformat(batch.batch_at.replace("Z", "+00:00")).astimezone(
        timezone.utc,  # noqa: UP017 - also imported by Spark's Python 3.10 image
    )
    return SnapshotIdentity(batch.batch_id, at.isoformat(), manifest_sha256)


def snapshot_action(current: SnapshotIdentity | None, incoming: SnapshotIdentity) -> str:
    if current is None:
        return "create"
    current_at = datetime.fromisoformat(current.batch_at)
    incoming_at = datetime.fromisoformat(incoming.batch_at)
    if incoming_at < current_at:
        return "stale"
    if incoming_at == current_at:
        if incoming != current:
            raise ValueError("conflicting snapshot identity at the current timestamp")
        return "replay"
    return "replace"
