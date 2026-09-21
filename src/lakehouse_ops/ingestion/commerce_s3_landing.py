from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import ClientError


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
        manifest, objects = _load_fixture(fixture)
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
                path.read_bytes(),
                checksum,
                metadata={"batch-id": batch_id, "source": "commerce"},
                content_type="application/x-ndjson",
            )

        manifest_path = fixture / "manifest.json"
        manifest_checksum = _file_sha256(manifest_path)
        created += self._put_once(
            f"{base_key}/manifest.json",
            manifest_path.read_bytes(),
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


def _load_fixture(fixture: Path) -> tuple[dict[str, Any], list[tuple[str, Path, str]]]:
    try:
        manifest = json.loads((fixture / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CommerceLandingError(f"fixture manifest is unreadable: {error}") from error

    batch_id = manifest.get("batch_id")
    tables = manifest.get("tables")
    if not isinstance(batch_id, str) or not batch_id or not isinstance(tables, dict) or not tables:
        raise CommerceLandingError("fixture manifest must contain batch_id and tables")

    objects: list[tuple[str, Path, str]] = []
    for table_name in sorted(tables):
        details = tables[table_name]
        if not isinstance(details, dict):
            raise CommerceLandingError(f"invalid manifest entry for table: {table_name}")
        file_name = details.get("file")
        checksum = details.get("sha256")
        if not isinstance(file_name, str) or Path(file_name).name != file_name:
            raise CommerceLandingError(f"invalid fixture file name for table: {table_name}")
        if not isinstance(checksum, str) or len(checksum) != 64:
            raise CommerceLandingError(f"invalid checksum for table: {table_name}")
        path = fixture / file_name
        if not path.is_file() or _file_sha256(path) != checksum:
            raise CommerceLandingError(f"fixture checksum verification failed: {path}")
        objects.append((file_name, path, checksum))
    return manifest, objects


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
