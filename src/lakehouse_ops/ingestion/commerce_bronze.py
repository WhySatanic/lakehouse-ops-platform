from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUIRED_TABLES = ("customers", "orders", "payments", "products")


class CommerceBronzeError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CommerceTableFile:
    name: str
    path: Path
    rows: int
    sha256: str


@dataclass(frozen=True, slots=True)
class CommerceBatchDirectory:
    batch_id: str
    batch_at: str
    tables: tuple[CommerceTableFile, ...]


def load_commerce_batch(path: Path, *, expected_batch_id: str) -> CommerceBatchDirectory:
    if not re.fullmatch(r"[0-9a-f]{16}", expected_batch_id):
        raise CommerceBronzeError("batch_id must contain 16 lowercase hexadecimal characters")
    if path.name != f"batch_id={expected_batch_id}":
        raise CommerceBronzeError("batch directory does not match batch_id")
    try:
        manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CommerceBronzeError(f"commerce manifest is unreadable: {error}") from error
    if manifest.get("schema_version") != 1 or manifest.get("batch_id") != expected_batch_id:
        raise CommerceBronzeError("commerce manifest identity is invalid")
    tables = manifest.get("tables")
    if not isinstance(tables, dict) or tuple(sorted(tables)) != REQUIRED_TABLES:
        raise CommerceBronzeError("commerce manifest must contain exactly four required tables")
    try:
        parsed_batch_at = datetime.fromisoformat(
            manifest["config"]["batch_at"].replace("Z", "+00:00")
        )
    except (KeyError, TypeError, ValueError) as error:
        raise CommerceBronzeError("commerce manifest batch_at is invalid") from error
    if parsed_batch_at.tzinfo is None:
        raise CommerceBronzeError("commerce manifest batch_at must include a timezone")

    verified = tuple(_verify_table(path, name, tables[name]) for name in REQUIRED_TABLES)
    return CommerceBatchDirectory(
        batch_id=expected_batch_id,
        batch_at=parsed_batch_at.astimezone(timezone.utc)  # noqa: UP017
        .isoformat()
        .replace("+00:00", "Z"),
        tables=verified,
    )


def _verify_table(root: Path, name: str, details: Any) -> CommerceTableFile:
    if not isinstance(details, dict):
        raise CommerceBronzeError(f"invalid manifest entry for table: {name}")
    file_name = details.get("file")
    rows = details.get("rows")
    checksum = details.get("sha256")
    if file_name != f"{name}.jsonl" or not isinstance(rows, int) or rows < 1:
        raise CommerceBronzeError(f"invalid manifest entry for table: {name}")
    if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
        raise CommerceBronzeError(f"invalid manifest checksum for table: {name}")
    table_path = root / file_name
    try:
        observed_checksum = _file_sha256(table_path)
        observed_rows = _line_count(table_path)
    except OSError as error:
        raise CommerceBronzeError(f"commerce table is unreadable: {table_path}") from error
    if observed_checksum != checksum:
        raise CommerceBronzeError(f"commerce table checksum mismatch: {name}")
    if observed_rows != rows:
        raise CommerceBronzeError(
            f"commerce table row count mismatch: {name} expected {rows}, observed {observed_rows}"
        )
    return CommerceTableFile(name=name, path=table_path, rows=rows, sha256=checksum)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _line_count(path: Path) -> int:
    count = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            count += chunk.count(b"\n")
    return count
