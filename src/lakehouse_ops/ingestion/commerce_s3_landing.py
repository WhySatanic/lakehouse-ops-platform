from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import ClientError

from lakehouse_ops.ingestion.commerce_batches import MAX_COMMERCE_MANIFEST_BYTES


class CommerceLandingError(ValueError):
    pass


class S3Client(Protocol):
    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class CommerceLandingResult:
    batch_id: str
    path: str
    objects: int
    created: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "created": self.created,
            "objects": self.objects,
            "path": self.path,
        }


class CommerceS3LandingZone:
    def __init__(self, client: S3Client, *, bucket: str, prefix: str = "landing") -> None:
        if not bucket:
            raise ValueError("bucket must not be empty")
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def write(self, fixture: Path) -> CommerceLandingResult:
        manifest, objects, verified_manifest = _load_fixture(fixture)
        batch_id = manifest["batch_id"]
        base_key = "/".join(
            part
            for part in (self._prefix, "source=commerce", f"batch_id={batch_id}")
            if part
        )
        created = 0

        # The manifest is the commit marker. Readers can ignore incomplete uploads because it
        # becomes visible only after every referenced table object has been verified or created.
        for name, path, checksum in objects:
            key = f"{base_key}/{name}"
            created += self._put_once(
                key,
                _read_verified_bytes(path, checksum),
                checksum,
                metadata={"batch-id": batch_id, "source": "commerce"},
                content_type="application/x-ndjson",
            )

        manifest_path = fixture / "manifest.json"
        try:
            manifest_body = _read_manifest_bytes(manifest_path)
        except OSError as error:
            raise CommerceLandingError("fixture manifest changed during upload") from error
        if manifest_body != verified_manifest:
            raise CommerceLandingError("fixture manifest changed during upload")
        manifest_checksum = hashlib.sha256(manifest_body).hexdigest()
        created += self._put_once(
            f"{base_key}/manifest.json",
            manifest_body,
            manifest_checksum,
            metadata={"batch-id": batch_id, "source": "commerce", "commit-marker": "true"},
            content_type="application/json",
        )
        return CommerceLandingResult(
            batch_id=batch_id,
            path=f"s3://{self._bucket}/{base_key}/",
            objects=len(objects) + 1,
            created=created,
        )

    def _put_once(
        self,
        key: str,
        body: bytes,
        checksum: str,
        *,
        metadata: dict[str, str],
        content_type: str,
    ) -> int:
        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=body,
                ContentType=content_type,
                Metadata={**metadata, "sha256": checksum},
                IfNoneMatch="*",
            )
            return 1
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = error.response.get("Error", {}).get("Code")
            if status != 412 and code not in {"PreconditionFailed", "412"}:
                raise

        existing = self._client.get_object(Bucket=self._bucket, Key=key)
        existing_checksum = existing.get("Metadata", {}).get("sha256")
        response_body = existing["Body"]
        try:
            existing_body = response_body.read()
        finally:
            response_body.close()
        observed_checksum = hashlib.sha256(existing_body).hexdigest()
        if existing_checksum != checksum or observed_checksum != checksum:
            raise CommerceLandingError(f"existing S3 object conflicts with fixture: {key}")
        return 0


def _load_fixture(
    fixture: Path,
) -> tuple[dict[str, Any], list[tuple[str, Path, str]], bytes]:
    manifest_path = fixture / "manifest.json"
    if manifest_path.is_symlink():
        raise CommerceLandingError(f"fixture manifest symlink is not allowed: {manifest_path}")
    try:
        manifest_body = _read_manifest_bytes(manifest_path)
        manifest = json.loads(manifest_body.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CommerceLandingError(f"fixture manifest is unreadable: {error}") from error

    if not isinstance(manifest, dict):
        raise CommerceLandingError("fixture manifest is invalid: expected an object")
    batch_id = manifest.get("batch_id")
    tables = manifest.get("tables")
    if type(manifest.get("schema_version")) is not int or manifest["schema_version"] != 1:
        raise CommerceLandingError("fixture schema version is unsupported")
    if not isinstance(batch_id, str) or not re.fullmatch(r"[0-9a-f]{16}", batch_id):
        raise CommerceLandingError("fixture batch_id is invalid")
    if not isinstance(tables, dict) or set(tables) != {
        "customers", "products", "orders", "payments"
    }:
        raise CommerceLandingError("fixture table inventory is invalid")

    objects: list[tuple[str, Path, str]] = []
    for table_name in sorted(tables):
        details = tables[table_name]
        if not isinstance(details, dict):
            raise CommerceLandingError(f"invalid manifest entry for table: {table_name}")
        file_name = details.get("file")
        checksum = details.get("sha256")
        rows = details.get("rows")
        if file_name != f"{table_name}.jsonl":
            raise CommerceLandingError("fixture table inventory is invalid")
        if type(rows) is not int or rows < 1:
            raise CommerceLandingError(f"invalid row count for table: {table_name}")
        if not isinstance(checksum, str) or len(checksum) != 64:
            raise CommerceLandingError(f"invalid checksum for table: {table_name}")
        path = fixture / file_name
        if path.is_symlink():
            raise CommerceLandingError(f"fixture table symlink is not allowed: {path}")
        if not path.is_file():
            raise CommerceLandingError(f"fixture checksum verification failed: {path}")
        observed_checksum, observed_rows = _file_digest_and_rows(path)
        if observed_checksum != checksum:
            raise CommerceLandingError(f"fixture checksum verification failed: {path}")
        if observed_rows != rows:
            raise CommerceLandingError(f"fixture row count mismatch: {path}")
        objects.append((file_name, path, checksum))
    return manifest, objects, manifest_body


def _read_manifest_bytes(path: Path) -> bytes:
    with path.open("rb") as stream:
        body = stream.read(MAX_COMMERCE_MANIFEST_BYTES + 1)
    if len(body) > MAX_COMMERCE_MANIFEST_BYTES:
        raise CommerceLandingError("fixture manifest exceeds 1 MiB")
    return body


def _read_verified_bytes(path: Path, checksum: str) -> bytes:
    try:
        body = path.read_bytes()
    except OSError as error:
        raise CommerceLandingError(f"fixture changed during upload: {path}") from error
    if hashlib.sha256(body).hexdigest() != checksum:
        raise CommerceLandingError(f"fixture changed during upload: {path}")
    return body


def _file_digest_and_rows(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    rows = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            rows += chunk.count(b"\n")
    return digest.hexdigest(), rows
