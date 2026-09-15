from __future__ import annotations

import gzip
import hashlib
import io
import json
import tarfile
from pathlib import Path, PurePosixPath
from typing import Any

from lakehouse_ops.digests import normalized_text_digest
from lakehouse_ops.report_schema import ReportSchemaError, validate_report_schema
from lakehouse_ops.trino_upgrade import load_upgrade_plan, validate_upgrade_report


class ReleaseCandidateError(RuntimeError):
    pass


DEFAULT_REPORT_SCHEMA = Path(
    "config/control-plane/schemas/release-candidate-bundle-report.schema.json"
)
DEFAULT_VERIFICATION_SCHEMA = Path(
    "config/control-plane/schemas/release-candidate-verification-report.schema.json"
)
MAX_ARCHIVE_MEMBERS = 256
MAX_MEMBER_BYTES = 64 * 1024 * 1024
MAX_ARCHIVE_BYTES = 256 * 1024 * 1024


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
    schema_path: Path = DEFAULT_REPORT_SCHEMA,
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
    report = {
        "schema_version": "1.0",
        "status": "ready",
        "target_release": "1.0.0",
        "source_revision": source_revision,
        "entries": len(archive_entries),
        "bundle_sha256": _digest(output_path),
    }
    try:
        validate_report_schema(report, schema_path)
    except ReportSchemaError as error:
        raise ReleaseCandidateError(str(error)) from error
    return report


def verify_release_candidate(
    *,
    bundle_path: Path,
    report_path: Path,
    expected_source_revision: str,
    report_schema_path: Path = DEFAULT_REPORT_SCHEMA,
    verification_schema_path: Path = DEFAULT_VERIFICATION_SCHEMA,
) -> dict[str, Any]:
    if not expected_source_revision.strip():
        raise ReleaseCandidateError("expected source revision must be non-empty")
    report = _load_object(report_path, "release candidate report")
    try:
        validate_report_schema(report, report_schema_path)
    except ReportSchemaError as error:
        raise ReleaseCandidateError(str(error)) from error
    if report["source_revision"] != expected_source_revision:
        raise ReleaseCandidateError("source revision does not match expected revision")

    bundle_digest = _digest(bundle_path)
    if report["bundle_sha256"] != bundle_digest:
        raise ReleaseCandidateError("bundle digest does not match report")
    members = _read_archive(bundle_path)
    manifest_content = members.pop("manifest.json", None)
    if manifest_content is None:
        raise ReleaseCandidateError("archive manifest is missing")
    manifest = _load_json_bytes(manifest_content, "archive manifest")
    manifest_entries = _validate_manifest(manifest, expected_source_revision)
    if report["entries"] != len(members) + 1:
        raise ReleaseCandidateError("archive entry count does not match report")
    if set(members) != set(manifest_entries):
        raise ReleaseCandidateError("archive membership does not match manifest")
    for name, expected_digest in manifest_entries.items():
        if _digest_bytes(members[name]) != expected_digest:
            raise ReleaseCandidateError(
                f"archive member digest does not match manifest: {name}"
            )

    verification = {
        "schema_version": "1.0",
        "status": "verified",
        "target_release": report["target_release"],
        "source_revision": report["source_revision"],
        "entries": report["entries"],
        "bundle_sha256": bundle_digest,
        "report_sha256": _digest(report_path),
        "manifest_sha256": _digest_bytes(manifest_content),
    }
    try:
        validate_report_schema(verification, verification_schema_path)
    except ReportSchemaError as error:
        raise ReleaseCandidateError(str(error)) from error
    return verification


def _load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReleaseCandidateError(f"cannot load {label}: {error}") from error
    if not isinstance(value, dict):
        raise ReleaseCandidateError(f"{label} must be a JSON object")
    return value


def _load_json_bytes(content: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseCandidateError(f"cannot load {label}: {error}") from error
    if not isinstance(value, dict):
        raise ReleaseCandidateError(f"{label} must be a JSON object")
    return value


def _read_archive(path: Path) -> dict[str, bytes]:
    try:
        with tarfile.open(path, "r:gz") as archive:
            members: dict[str, bytes] = {}
            total_size = 0
            for info in archive:
                if len(members) >= MAX_ARCHIVE_MEMBERS:
                    raise ReleaseCandidateError("archive has too many members")
                _validate_archive_path(info.name)
                if info.name in members:
                    raise ReleaseCandidateError(f"duplicate archive member: {info.name}")
                if not info.isfile():
                    raise ReleaseCandidateError(
                        f"archive member must be a regular file: {info.name}"
                    )
                if info.size > MAX_MEMBER_BYTES:
                    raise ReleaseCandidateError(f"archive member is too large: {info.name}")
                total_size += info.size
                if total_size > MAX_ARCHIVE_BYTES:
                    raise ReleaseCandidateError("archive expands beyond the size limit")
                source = archive.extractfile(info)
                if source is None:
                    raise ReleaseCandidateError(f"cannot read archive member: {info.name}")
                content = source.read(MAX_MEMBER_BYTES + 1)
                if len(content) != info.size:
                    raise ReleaseCandidateError(f"archive member size is invalid: {info.name}")
                members[info.name] = content
            return members
    except ReleaseCandidateError:
        raise
    except (OSError, tarfile.TarError) as error:
        raise ReleaseCandidateError(f"cannot read release candidate archive: {error}") from error


def _validate_archive_path(name: str) -> None:
    path = PurePosixPath(name)
    if (
        not name
        or "\\" in name
        or path.is_absolute()
        or "." in path.parts
        or ".." in path.parts
        or path.as_posix() != name
    ):
        raise ReleaseCandidateError(f"archive member path is unsafe: {name}")


def _validate_manifest(manifest: dict[str, Any], source_revision: str) -> dict[str, str]:
    required = {
        "schema_version",
        "status",
        "target_release",
        "source_revision",
        "entries",
    }
    if set(manifest) != required:
        raise ReleaseCandidateError("archive manifest fields are invalid")
    if (
        manifest["schema_version"] != "1.0"
        or manifest["status"] != "ready"
        or manifest["target_release"] != "1.0.0"
        or manifest["source_revision"] != source_revision
    ):
        raise ReleaseCandidateError("archive manifest identity is invalid")
    entries = manifest["entries"]
    if not isinstance(entries, list) or not entries:
        raise ReleaseCandidateError("archive manifest entries must be a non-empty array")
    verified: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"path", "sha256"}:
            raise ReleaseCandidateError("archive manifest entry fields are invalid")
        name = entry["path"]
        digest = entry["sha256"]
        if not isinstance(name, str) or not isinstance(digest, str):
            raise ReleaseCandidateError("archive manifest entry values are invalid")
        _validate_archive_path(name)
        if name == "manifest.json" or name in verified:
            raise ReleaseCandidateError(f"duplicate archive manifest entry: {name}")
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ReleaseCandidateError(f"archive manifest digest is invalid: {name}")
        verified[name] = digest
    return verified


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
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ReleaseCandidateError(f"cannot read {path}: {error}") from error
    return digest.hexdigest()


def _read_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError as error:
        raise ReleaseCandidateError(f"cannot read {path}: {error}") from error


def _digest_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()
