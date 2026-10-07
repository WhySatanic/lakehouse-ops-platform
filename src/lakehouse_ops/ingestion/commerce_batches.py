from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from lakehouse_ops.commerce_runner_lock import CommerceRunnerBusyError, commerce_runner_lock


class CommerceBatchError(ValueError):
    pass


class S3Client(Protocol):
    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class CommerceBatch:
    batch_id: str
    batch_at: str
    manifest_sha256: str
    path: str
    committed_at: datetime | None = None

    def as_dict(self) -> dict[str, str]:
        return {
            "batch_at": self.batch_at,
            "batch_id": self.batch_id,
            "manifest_sha256": self.manifest_sha256,
            "path": self.path,
        }


class CommerceBatchPlanner:
    def __init__(
        self,
        client: S3Client,
        *,
        bucket: str,
        state_path: Path = Path("data/state/commerce-batches.json"),
        prefix: str = "landing",
    ) -> None:
        if not bucket:
            raise ValueError("bucket must not be empty")
        self._client = client
        self._bucket = bucket
        self._state_path = state_path
        self._prefix = prefix.strip("/")

    def plan(
        self, *, max_batches: int, replay_batches: tuple[str, ...] = ()
    ) -> dict[str, Any]:
        if max_batches < 1:
            raise CommerceBatchError("max_batches must be positive")
        if len(replay_batches) > max_batches:
            raise CommerceBatchError("replay batch count exceeds max_batches")
        if len(set(replay_batches)) != len(replay_batches):
            raise CommerceBatchError("replay batches must be unique")

        batches = self.discover()
        by_id = {batch.batch_id: batch for batch in batches}
        state = _load_state(self._state_path)
        _verify_processed_content(state, by_id)
        if replay_batches:
            missing = [batch_id for batch_id in replay_batches if batch_id not in by_id]
            if missing:
                raise CommerceBatchError(
                    f"replay batch is not committed: {', '.join(missing)}"
                )
            selected = [by_id[batch_id] for batch_id in replay_batches]
            mode = "replay"
        else:
            processed = state["processed_batches"]
            selected = [
                batch for batch in batches if batch.batch_id not in processed
            ][:max_batches]
            mode = "incremental"
        return {
            "batches": [batch.as_dict() for batch in selected],
            "committed_batches": len(batches),
            "mode": mode,
            "processed_batches": len(state["processed_batches"]),
            "selected_batches": len(selected),
        }

    def commit(
        self, batch_id: str, *, expected_manifest_sha256: str | None = None,
    ) -> dict[str, Any]:
        try:
            with commerce_runner_lock(Path(f"{self._state_path}.lock")):
                return self._commit_locked(batch_id, expected_manifest_sha256)
        except CommerceRunnerBusyError as error:
            raise CommerceBatchError(f"commerce checkpoint is busy: {self._state_path}") from error

    def _commit_locked(
        self, batch_id: str, expected_manifest_sha256: str | None,
    ) -> dict[str, Any]:
        ordered_batches = self.discover()
        batches = {batch.batch_id: batch for batch in ordered_batches}
        if batch_id not in batches:
            raise CommerceBatchError(f"batch is not committed: {batch_id}")
        state = _load_state(self._state_path)
        _verify_processed_content(state, batches)
        batch = batches[batch_id]
        if (
            expected_manifest_sha256 is not None
            and batch.manifest_sha256 != expected_manifest_sha256
        ):
            raise CommerceBatchError("committed manifest changed during processing")
        processed = state["processed_batches"]
        existing = processed.get(batch_id)
        if existing:
            return {"batch_id": batch_id, "created": False, "state": str(self._state_path)}
        next_batch = next(
            batch for batch in ordered_batches if batch.batch_id not in processed
        )
        if next_batch.batch_id != batch_id:
            raise CommerceBatchError(
                f"cannot skip earlier unprocessed batch: {next_batch.batch_id}"
            )
        processed[batch_id] = {
            "batch_at": batch.batch_at,
            "manifest_sha256": batch.manifest_sha256,
        }
        _write_state(self._state_path, state)
        return {"batch_id": batch_id, "created": True, "state": str(self._state_path)}

    def check_source_freshness(
        self, *, max_age_seconds: int, now: datetime | None = None
    ) -> dict[str, Any]:
        if max_age_seconds < 1:
            raise CommerceBatchError("max_age_seconds must be positive")
        observed_at = now or datetime.now(UTC)
        if observed_at.tzinfo is None:
            raise CommerceBatchError("observation time must include a timezone")
        batches = self.discover()
        if not batches:
            return {
                "status": "stale",
                "reason": "no_committed_batches",
                "latest_batch_id": None,
                "latest_committed_at": None,
                "age_seconds": None,
                "max_age_seconds": max_age_seconds,
            }
        timestamped = [
            (batch.committed_at, batch)
            for batch in batches
            if batch.committed_at is not None
        ]
        if len(timestamped) != len(batches):
            raise CommerceBatchError("S3 listing is missing a committed manifest timestamp")
        committed_at, latest = max(timestamped, key=lambda item: item[0])
        age_seconds = max(0, math.ceil((observed_at - committed_at).total_seconds()))
        stale = age_seconds > max_age_seconds
        return {
            "status": "stale" if stale else "ready",
            "reason": "age_limit_exceeded" if stale else None,
            "latest_batch_id": latest.batch_id,
            "latest_committed_at": committed_at.astimezone(UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "age_seconds": age_seconds,
            "max_age_seconds": max_age_seconds,
        }

    def check_backlog_freshness(
        self, *, max_age_seconds: int, now: datetime | None = None
    ) -> dict[str, Any]:
        if max_age_seconds < 1:
            raise CommerceBatchError("max_age_seconds must be positive")
        observed_at = now or datetime.now(UTC)
        if observed_at.tzinfo is None:
            raise CommerceBatchError("observation time must include a timezone")
        batches = self.discover()
        by_id = {batch.batch_id: batch for batch in batches}
        state = _load_state(self._state_path)
        _verify_processed_content(state, by_id)
        pending = [
            batch for batch in batches if batch.batch_id not in state["processed_batches"]
        ]
        if not pending:
            return {
                "status": "ready",
                "reason": None,
                "pending_batches": 0,
                "oldest_pending_batch_id": None,
                "oldest_committed_at": None,
                "age_seconds": None,
                "max_age_seconds": max_age_seconds,
            }
        timestamped = [
            (batch.committed_at, batch)
            for batch in pending
            if batch.committed_at is not None
        ]
        if len(timestamped) != len(pending):
            raise CommerceBatchError("S3 listing is missing a committed manifest timestamp")
        committed_at, oldest = min(timestamped, key=lambda item: item[0])
        age_seconds = max(0, math.ceil((observed_at - committed_at).total_seconds()))
        stale = age_seconds > max_age_seconds
        return {
            "status": "stale" if stale else "ready",
            "reason": "pending_age_limit_exceeded" if stale else None,
            "pending_batches": len(pending),
            "oldest_pending_batch_id": oldest.batch_id,
            "oldest_committed_at": committed_at.astimezone(UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "age_seconds": age_seconds,
            "max_age_seconds": max_age_seconds,
        }

    def discover(self) -> list[CommerceBatch]:
        root = "/".join(
            part for part in (self._prefix, "source=commerce") if part
        )
        manifest_pattern = re.compile(
            rf"^{re.escape(root)}/batch_id=([0-9a-f]{{16}})/manifest\.json$"
        )
        keys: list[tuple[str, str, object]] = []
        continuation: str | None = None
        seen_tokens: set[str] = set()
        while True:
            request: dict[str, Any] = {"Bucket": self._bucket, "Prefix": f"{root}/"}
            if continuation:
                request["ContinuationToken"] = continuation
            response = self._client.list_objects_v2(**request)
            for item in response.get("Contents", []):
                key = item.get("Key", "")
                match = manifest_pattern.fullmatch(key)
                if match:
                    keys.append((match.group(1), key, item.get("LastModified")))
            if not response.get("IsTruncated"):
                break
            continuation = response.get("NextContinuationToken")
            if not continuation:
                raise CommerceBatchError("S3 listing is truncated without a continuation token")
            if continuation in seen_tokens:
                raise CommerceBatchError("S3 listing returned a repeated continuation token")
            seen_tokens.add(continuation)

        batches = [
            self._read_manifest(batch_id, key, committed_at)
            for batch_id, key, committed_at in keys
        ]
        return sorted(batches, key=lambda batch: (batch.batch_at, batch.batch_id))

    def _read_manifest(self, batch_id: str, key: str, committed_at: object) -> CommerceBatch:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        metadata = response.get("Metadata", {})
        body_stream = response["Body"]
        try:
            body = body_stream.read()
        finally:
            body_stream.close()
        checksum = hashlib.sha256(body).hexdigest()
        if (
            metadata.get("commit-marker") != "true"
            or metadata.get("source") != "commerce"
            or metadata.get("batch-id") != batch_id
            or metadata.get("sha256") != checksum
        ):
            raise CommerceBatchError(f"invalid commerce commit marker: {key}")
        try:
            manifest = json.loads(body)
            batch_at = manifest["config"]["batch_at"]
            parsed_batch_at = datetime.fromisoformat(batch_at.replace("Z", "+00:00"))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            raise CommerceBatchError(f"invalid commerce manifest: {key}") from error
        if (
            type(manifest.get("schema_version")) is not int
            or manifest["schema_version"] != 1
            or manifest.get("batch_id") != batch_id
            or parsed_batch_at.tzinfo is None
            or not _valid_table_inventory(manifest.get("tables"))
        ):
            raise CommerceBatchError(f"invalid commerce manifest: {key}")
        return CommerceBatch(
            batch_id=batch_id,
            batch_at=parsed_batch_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            manifest_sha256=checksum,
            path=f"s3://{self._bucket}/{key.removesuffix('manifest.json')}",
            committed_at=(
                committed_at
                if isinstance(committed_at, datetime) and committed_at.tzinfo
                else None
            ),
        )


def _valid_table_inventory(tables: object) -> bool:
    if not isinstance(tables, dict) or set(tables) != {
        "customers", "products", "orders", "payments"
    }:
        return False
    for name, details in tables.items():
        if not isinstance(details, dict):
            return False
        checksum = details.get("sha256")
        rows = details.get("rows")
        if (
            details.get("file") != f"{name}.jsonl"
            or type(rows) is not int
            or rows < 1
            or not isinstance(checksum, str)
            or not re.fullmatch(r"[0-9a-f]{64}", checksum)
        ):
            return False
    return True


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "processed_batches": {}}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CommerceBatchError(f"commerce checkpoint is unreadable: {error}") from error
    if not isinstance(state, dict) or state.get("schema_version") != 1 or not isinstance(
        state.get("processed_batches"), dict
    ):
        raise CommerceBatchError("commerce checkpoint has an unsupported structure")
    for batch_id, details in state["processed_batches"].items():
        if (
            not re.fullmatch(r"[0-9a-f]{16}", batch_id)
            or not isinstance(details, dict)
            or not isinstance(details.get("manifest_sha256"), str)
        ):
            raise CommerceBatchError("commerce checkpoint has an unsupported structure")
    return state


def _verify_processed_content(
    state: dict[str, Any], batches: dict[str, CommerceBatch]
) -> None:
    for batch_id, details in state["processed_batches"].items():
        batch = batches.get(batch_id)
        if not batch:
            raise CommerceBatchError(
                f"processed batch commit marker is missing: {batch_id}"
            )
        if details["manifest_sha256"] != batch.manifest_sha256:
            raise CommerceBatchError(
                f"committed manifest changed after processing: {batch_id}"
            )


def _write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, indent=2, sort_keys=True) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent, text=True
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
