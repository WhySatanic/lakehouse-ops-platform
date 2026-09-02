from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

from lakehouse_ops.digests import normalized_text_digest

DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
COMPOSE_IMAGE = re.compile(r"^\s*image:\s*(?P<reference>\S+)\s*$")
VARIABLE_DEFAULT = re.compile(r"^\$\{[^:}]+:-(?P<default>[^}]+)}$")
FROM_IMAGE = re.compile(r"^\s*FROM\s+(?:--platform=\S+\s+)?(?P<reference>\S+)", re.I)
LOCAL_PREFIX = "lakehouse-ops/"


class ImageLockError(ValueError):
    pass


def verify_image_lock(
    lock_path: Path,
    compose_path: Path,
    dockerfiles: list[Path],
    upgrade_plan_path: Path,
) -> dict[str, Any]:
    locked = _load_lock(lock_path)
    uses = [
        *_compose_uses(compose_path),
        *(use for path in dockerfiles for use in _dockerfile_uses(path)),
        *_upgrade_uses(upgrade_plan_path),
    ]
    used_tags: set[str] = set()

    for use in uses:
        tag, digest = _split_pinned_reference(use["reference"], use["source"])
        expected = locked.get(tag)
        if expected is None:
            raise ImageLockError(f"image is not present in lock: {tag} ({use['source']})")
        if digest != expected:
            raise ImageLockError(f"image digest does not match lock: {tag} ({use['source']})")
        used_tags.add(tag)

    stale = sorted(set(locked) - used_tags)
    if stale:
        raise ImageLockError(f"lock contains unused images: {', '.join(stale)}")

    source_counts = Counter(use["kind"] for use in uses)
    return {
        "schema_version": "1.0",
        "status": "ready",
        "images": len(used_tags),
        "uses": len(uses),
        "sources": dict(sorted(source_counts.items())),
        "lock_sha256": normalized_text_digest(lock_path),
    }


def _load_lock(path: Path) -> dict[str, str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ImageLockError(f"cannot read image lock: {error}") from error
    if not isinstance(payload, dict) or payload.get("schema_version") != "1.0":
        raise ImageLockError("image lock schema_version must be 1.0")
    entries = payload.get("images")
    if not isinstance(entries, list) or not entries:
        raise ImageLockError("image lock must contain a non-empty images array")

    locked: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ImageLockError("image lock entries must be objects")
        tag = entry.get("tag")
        digest = entry.get("digest")
        if not isinstance(tag, str) or not _has_tag(tag):
            raise ImageLockError("every locked image must retain an explicit tag")
        if not isinstance(digest, str) or DIGEST.fullmatch(digest) is None:
            raise ImageLockError(f"invalid digest for locked image: {tag}")
        if tag in locked:
            raise ImageLockError(f"duplicate locked image: {tag}")
        locked[tag] = digest
    return locked


def _compose_uses(path: Path) -> list[dict[str, str]]:
    uses: list[dict[str, str]] = []
    for line_number, line in enumerate(_read_lines(path), start=1):
        match = COMPOSE_IMAGE.match(line)
        if match is None:
            continue
        reference = _variable_default(match.group("reference"))
        if reference.startswith(LOCAL_PREFIX):
            continue
        uses.append(
            {
                "kind": "compose",
                "source": f"{path}:{line_number}",
                "reference": reference,
            }
        )
    return uses


def _dockerfile_uses(path: Path) -> list[dict[str, str]]:
    uses: list[dict[str, str]] = []
    for line_number, line in enumerate(_read_lines(path), start=1):
        match = FROM_IMAGE.match(line)
        if match is not None:
            uses.append(
                {
                    "kind": "dockerfile",
                    "source": f"{path}:{line_number}",
                    "reference": match.group("reference"),
                }
            )
    if not uses:
        raise ImageLockError(f"Dockerfile has no FROM instruction: {path}")
    return uses


def _upgrade_uses(path: Path) -> list[dict[str, str]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        references = [payload[phase]["image"] for phase in ("source", "target")]
    except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise ImageLockError(f"cannot read upgrade image references: {error}") from error
    if not all(isinstance(reference, str) for reference in references):
        raise ImageLockError("upgrade image references must be strings")
    return [
        {"kind": "upgrade", "source": f"{path}:{phase}", "reference": reference}
        for phase, reference in zip(("source", "target"), references, strict=True)
    ]


def _split_pinned_reference(reference: str, source: str) -> tuple[str, str]:
    if "@" not in reference:
        raise ImageLockError(f"external image is not digest-pinned: {reference} ({source})")
    tag, digest = reference.rsplit("@", 1)
    if not _has_tag(tag) or DIGEST.fullmatch(digest) is None:
        raise ImageLockError(f"invalid pinned image reference: {reference} ({source})")
    return tag, digest


def _has_tag(reference: str) -> bool:
    return ":" in reference.rsplit("/", 1)[-1]


def _variable_default(reference: str) -> str:
    match = VARIABLE_DEFAULT.fullmatch(reference)
    return match.group("default") if match is not None else reference


def _read_lines(path: Path) -> list[str]:
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ImageLockError(f"cannot read image source {path}: {error}") from error
