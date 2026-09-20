from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any


class AuthorizationEvidenceError(RuntimeError):
    pass


REQUIRED_AUTHORIZATION_EVIDENCE = tuple(
    sorted(
        {
            "break-glass-allowed.json",
            "break-glass-audit.json",
            "break-glass-denied.json",
            "break-glass-drill.json",
            "break-glass-grant.json",
            "break-glass-revoke.json",
            "ranger-audit.json",
            "ranger-policy-resync.json",
            "ranger-policy-sync.json",
            "trino-authenticated-authorization-report.json",
            "trino-authenticated-metadata-db-recovery.json",
            "trino-authenticated-metastore-recovery.json",
            "trino-authenticated-worker-recovery.json",
            "trino-authenticated-worker-shutdown.json",
            "trino-authorization-report.json",
        }
    )
)
MAX_EVIDENCE_FILE_BYTES = 16 * 1024 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}")
_REVISION = re.compile(r"[0-9a-f]{40}")


def build_authorization_evidence_manifest(
    evidence_root: Path, *, source_revision: str
) -> dict[str, Any]:
    _validate_revision(source_revision)
    files = [_file_entry(evidence_root, name) for name in REQUIRED_AUTHORIZATION_EVIDENCE]
    report = {
        "schema_version": "1.0",
        "status": "ready",
        "source_revision": source_revision,
        "files": files,
        "evidence_set_sha256": _evidence_set_digest(files),
    }
    validate_authorization_evidence_manifest(
        report, evidence_root, expected_source_revision=source_revision
    )
    return report


def validate_authorization_evidence_manifest(
    report: dict[str, Any],
    evidence_root: Path,
    *,
    expected_source_revision: str | None = None,
    strict_membership: bool = False,
) -> dict[str, Any]:
    if report.get("schema_version") != "1.0" or report.get("status") != "ready":
        raise AuthorizationEvidenceError("manifest is not ready schema 1.0 evidence")
    source_revision = report.get("source_revision")
    if not isinstance(source_revision, str):
        raise AuthorizationEvidenceError("manifest source revision is invalid")
    _validate_revision(source_revision)
    if expected_source_revision is not None and source_revision != expected_source_revision:
        raise AuthorizationEvidenceError("manifest source revision does not match expected")

    files = report.get("files")
    if not isinstance(files, list):
        raise AuthorizationEvidenceError("manifest files must be an array")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in files:
        if not isinstance(entry, dict):
            raise AuthorizationEvidenceError("manifest file entry must be an object")
        name = entry.get("path")
        size = entry.get("size_bytes")
        digest = entry.get("sha256")
        if not isinstance(name, str) or name not in REQUIRED_AUTHORIZATION_EVIDENCE:
            raise AuthorizationEvidenceError(f"unexpected authorization evidence: {name}")
        if name in seen:
            raise AuthorizationEvidenceError(f"duplicate authorization evidence: {name}")
        valid_size = (
            isinstance(size, int)
            and not isinstance(size, bool)
            and 0 <= size <= MAX_EVIDENCE_FILE_BYTES
        )
        if not valid_size:
            raise AuthorizationEvidenceError(f"invalid authorization evidence size: {name}")
        if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
            raise AuthorizationEvidenceError(f"invalid authorization evidence digest: {name}")
        actual = _file_entry(evidence_root, name)
        if actual != entry:
            raise AuthorizationEvidenceError(f"authorization evidence mismatch: {name}")
        seen.add(name)
        normalized.append(actual)

    expected = set(REQUIRED_AUTHORIZATION_EVIDENCE)
    if seen != expected:
        missing = sorted(expected - seen)
        raise AuthorizationEvidenceError(f"authorization evidence is incomplete: {missing}")
    if strict_membership:
        allowed = expected | {"authorization-evidence-manifest.json"}
        actual = {path.name for path in evidence_root.iterdir() if path.is_file()}
        if actual != allowed:
            unexpected = sorted(actual - allowed)
            missing = sorted(allowed - actual)
            raise AuthorizationEvidenceError(
                "authorization artifact membership mismatch; "
                f"missing={missing}, unexpected={unexpected}"
            )
    normalized.sort(key=lambda item: item["path"])
    evidence_set_digest = report.get("evidence_set_sha256")
    if evidence_set_digest != _evidence_set_digest(normalized):
        raise AuthorizationEvidenceError("authorization evidence set digest is invalid")
    return {
        "status": "verified",
        "source_revision": source_revision,
        "files": len(normalized),
        "evidence_set_sha256": evidence_set_digest,
    }


def _file_entry(root: Path, name: str) -> dict[str, Any]:
    path = root / name
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise AuthorizationEvidenceError(
            f"cannot resolve authorization evidence {name}: {error}"
        ) from error
    if path.is_symlink() or root.resolve() not in resolved.parents:
        raise AuthorizationEvidenceError(f"authorization evidence escapes root: {name}")
    try:
        payload = resolved.read_bytes()
    except OSError as error:
        raise AuthorizationEvidenceError(
            f"cannot read authorization evidence {name}: {error}"
        ) from error
    if len(payload) > MAX_EVIDENCE_FILE_BYTES:
        raise AuthorizationEvidenceError(f"authorization evidence exceeds size limit: {name}")
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as error:
        raise AuthorizationEvidenceError(
            f"authorization evidence is not valid JSON: {name}"
        ) from error
    if not isinstance(value, dict):
        raise AuthorizationEvidenceError(f"authorization evidence must be an object: {name}")
    return {
        "path": name,
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def _evidence_set_digest(files: list[dict[str, Any]]) -> str:
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def _validate_revision(source_revision: str) -> None:
    if _REVISION.fullmatch(source_revision) is None:
        raise AuthorizationEvidenceError("source revision must be a lowercase full Git SHA")
