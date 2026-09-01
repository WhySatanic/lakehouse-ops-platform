from __future__ import annotations

import gzip
import hashlib
import io
import json
import tarfile
from pathlib import Path
from typing import Any

from lakehouse_ops.digests import normalized_text_digest
from lakehouse_ops.trino_upgrade import load_upgrade_plan, validate_upgrade_report


class ReleaseCandidateError(RuntimeError):
    pass


def build_release_candidate(
    *,
    evidence_root: Path,
    attestation_path: Path,
    readiness_contract_path: Path,
    control_plane_contract_path: Path,
    upgrade_report_path: Path,
    upgrade_plan_path: Path,
    source_revision: str,
    output_path: Path,
) -> dict[str, Any]:
    if not source_revision.strip():
        raise ReleaseCandidateError("source revision must be non-empty")
    attestation = _load_object(attestation_path, "release readiness attestation")
    if (
        attestation.get("schema_version") != "1.0"
        or attestation.get("status") != "ready"
        or attestation.get("target_release") != "1.0.0"
    ):
        raise ReleaseCandidateError("release readiness attestation is not ready")
    if attestation.get("source_revision") != source_revision:
        raise ReleaseCandidateError("attestation source revision does not match checkout")
    if attestation.get("contract_sha256") != normalized_text_digest(
        readiness_contract_path
    ):
        raise ReleaseCandidateError("readiness contract digest does not match attestation")

    archive_entries: dict[str, bytes] = {
        "attestation/release-readiness.json": _read_bytes(attestation_path),
        "contracts/readiness-contract.json": _read_bytes(readiness_contract_path),
        "contracts/control-plane-contract.json": _read_bytes(control_plane_contract_path),
        "upgrade/upgrade-plan.json": _read_bytes(upgrade_plan_path),
        "upgrade/trino-upgrade-rehearsal.json": _read_bytes(upgrade_report_path),
    }
    evidence = attestation.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        raise ReleaseCandidateError("attestation evidence must be a non-empty array")
    for entry in evidence:
        if not isinstance(entry, dict):
            raise ReleaseCandidateError("attestation evidence entry must be an object")
        relative_path = entry.get("path")
        expected_digest = entry.get("sha256")
        if not isinstance(relative_path, str) or not isinstance(expected_digest, str):
            raise ReleaseCandidateError("attestation evidence fields are invalid")
        path = _resolve_below(evidence_root, relative_path)
        if _digest(path) != expected_digest:
            raise ReleaseCandidateError(f"evidence digest changed: {relative_path}")
        archive_entries[f"evidence/{relative_path}"] = _read_bytes(path)

    try:
        upgrade_report = _load_object(upgrade_report_path, "Trino upgrade report")
        upgrade_plan = load_upgrade_plan(upgrade_plan_path)
        validate_upgrade_report(upgrade_report, upgrade_plan)
    except (OSError, ValueError, RuntimeError) as error:
        raise ReleaseCandidateError(f"upgrade and rollback evidence is invalid: {error}") from error

    manifest = {
        "schema_version": "1.0",
        "status": "ready",
        "target_release": "1.0.0",
        "source_revision": source_revision,
        "entries": [
            {"path": name, "sha256": _digest_bytes(content)}
            for name, content in sorted(archive_entries.items())
        ],
    }
    archive_entries["manifest.json"] = (
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    ).encode()
    _write_deterministic_archive(output_path, archive_entries)
    return {
        "schema_version": "1.0",
        "status": "ready",
        "target_release": "1.0.0",
        "source_revision": source_revision,
        "entries": len(archive_entries),
        "bundle_sha256": _digest(output_path),
    }


def _load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReleaseCandidateError(f"cannot load {label}: {error}") from error
    if not isinstance(value, dict):
        raise ReleaseCandidateError(f"{label} must be a JSON object")
    return value


def _resolve_below(root: Path, relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ReleaseCandidateError(f"evidence path must remain below root: {relative_path}")
    root = root.resolve()
    path = (root / relative).resolve()
    if root not in path.parents:
        raise ReleaseCandidateError(f"evidence path escapes root: {relative_path}")
    return path


def _write_deterministic_archive(path: Path, entries: dict[str, bytes]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with (
        path.open("wb") as raw,
        gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed,
        tarfile.open(fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT) as archive,
    ):
        for name, content in sorted(entries.items()):
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = 0o644
            info.mtime = 0
            archive.addfile(info, io.BytesIO(content))


def _digest(path: Path) -> str:
    return _digest_bytes(_read_bytes(path))


def _read_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError as error:
        raise ReleaseCandidateError(f"cannot read {path}: {error}") from error


def _digest_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()
