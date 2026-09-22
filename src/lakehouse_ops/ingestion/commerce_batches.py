from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol


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
        state_path: Path,
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

    def commit(self, batch_id: str) -> dict[str, Any]:
        batches = {batch.batch_id: batch for batch in self.discover()}
        if batch_id not in batches:
            raise CommerceBatchError(f"batch is not committed: {batch_id}")
        state = _load_state(self._state_path)
        _verify_processed_content(state, batches)
        batch = batches[batch_id]
        processed = state["processed_batches"]
        existing = processed.get(batch_id)
        if existing:
            return {"batch_id": batch_id, "created": False, "state": str(self._state_path)}
        processed[batch_id] = {
            "batch_at": batch.batch_at,
            "manifest_sha256": batch.manifest_sha256,
        }
        _write_state(self._state_path, state)
        return {"batch_id": batch_id, "created": True, "state": str(self._state_path)}

    def discover(self) -> list[CommerceBatch]:
        root = "/".join(
            part for part in (self._prefix, "source=commerce") if part
        )
        manifest_pattern = re.compile(
            rf"^{re.escape(root)}/batch_id=([0-9a-f]{{16}})/manifest\.json$"
        )
        keys: list[tuple[str, str]] = []
        continuation: str | None = None
        while True:
            request: dict[str, Any] = {"Bucket": self._bucket, "Prefix": f"{root}/"}
            if continuation:
                request["ContinuationToken"] = continuation
            response = self._client.list_objects_v2(**request)
            for item in response.get("Contents", []):
                key = item.get("Key", "")
                match = manifest_pattern.fullmatch(key)
                if match:
                    keys.append((match.group(1), key))
            if not response.get("IsTruncated"):
                break
            continuation = response.get("NextContinuationToken")
            if not continuation:
                raise CommerceBatchError("S3 listing is truncated without a continuation token")

        batches = [self._read_manifest(batch_id, key) for batch_id, key in keys]
        return sorted(batches, key=lambda batch: (batch.batch_at, batch.batch_id))

    def _read_manifest(self, batch_id: str, key: str) -> CommerceBatch:
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
            manifest.get("batch_id") != batch_id
            or parsed_batch_at.tzinfo is None
            or not manifest.get("tables")
        ):
            raise CommerceBatchError(f"invalid commerce manifest: {key}")
        return CommerceBatch(
            batch_id=batch_id,
            batch_at=parsed_batch_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            manifest_sha256=checksum,
            path=f"s3://{self._bucket}/{key.removesuffix('manifest.json')}",
        )


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "processed_batches": {}}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CommerceBatchError(f"commerce checkpoint is unreadable: {error}") from error
    if state.get("schema_version") != 1 or not isinstance(
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
