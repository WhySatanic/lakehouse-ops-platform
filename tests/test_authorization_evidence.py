from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakehouse_ops.authorization_evidence import (
    REQUIRED_AUTHORIZATION_EVIDENCE,
    AuthorizationEvidenceError,
    build_authorization_evidence_manifest,
    validate_authorization_evidence_manifest,
)

REVISION = "a" * 40


def evidence_root(tmp_path: Path) -> Path:
    root = tmp_path / "ranger-evidence"
    root.mkdir()
    for index, name in enumerate(REQUIRED_AUTHORIZATION_EVIDENCE):
        (root / name).write_text(
            json.dumps({"schema_version": "1.0", "index": index}) + "\n",
            encoding="utf-8",
        )
    return root


def test_manifest_round_trip_binds_every_required_report(tmp_path: Path) -> None:
    root = evidence_root(tmp_path)
    manifest = build_authorization_evidence_manifest(root, source_revision=REVISION)

    result = validate_authorization_evidence_manifest(
        manifest, root, expected_source_revision=REVISION
    )

    assert result == {
        "status": "verified",
        "source_revision": REVISION,
        "files": len(REQUIRED_AUTHORIZATION_EVIDENCE),
        "evidence_set_sha256": manifest["evidence_set_sha256"],
    }
    assert [entry["path"] for entry in manifest["files"]] == list(
        REQUIRED_AUTHORIZATION_EVIDENCE
    )


def test_manifest_rejects_tampered_evidence(tmp_path: Path) -> None:
    root = evidence_root(tmp_path)
    manifest = build_authorization_evidence_manifest(root, source_revision=REVISION)
    target = root / REQUIRED_AUTHORIZATION_EVIDENCE[0]
    target.write_text('{"tampered":true}\n', encoding="utf-8")

    with pytest.raises(AuthorizationEvidenceError, match="mismatch"):
        validate_authorization_evidence_manifest(manifest, root)


def test_manifest_rejects_missing_evidence(tmp_path: Path) -> None:
    root = evidence_root(tmp_path)
    manifest = build_authorization_evidence_manifest(root, source_revision=REVISION)
    manifest["files"].pop()

    with pytest.raises(AuthorizationEvidenceError, match="incomplete"):
        validate_authorization_evidence_manifest(manifest, root)


def test_manifest_rejects_unexpected_path(tmp_path: Path) -> None:
    root = evidence_root(tmp_path)
    manifest = build_authorization_evidence_manifest(root, source_revision=REVISION)
    manifest["files"][0]["path"] = "../outside.json"

    with pytest.raises(AuthorizationEvidenceError, match="unexpected"):
        validate_authorization_evidence_manifest(manifest, root)


def test_strict_manifest_rejects_extra_artifact_member(tmp_path: Path) -> None:
    root = evidence_root(tmp_path)
    manifest = build_authorization_evidence_manifest(root, source_revision=REVISION)
    (root / "authorization-evidence-manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    (root / "unexpected.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(AuthorizationEvidenceError, match=r"unexpected\.json"):
        validate_authorization_evidence_manifest(
            manifest, root, strict_membership=True
        )


def test_manifest_rejects_wrong_source_revision(tmp_path: Path) -> None:
    root = evidence_root(tmp_path)
    manifest = build_authorization_evidence_manifest(root, source_revision=REVISION)

    with pytest.raises(AuthorizationEvidenceError, match="does not match"):
        validate_authorization_evidence_manifest(
            manifest, root, expected_source_revision="b" * 40
        )


def test_manifest_builder_rejects_non_object_json(tmp_path: Path) -> None:
    root = evidence_root(tmp_path)
    (root / REQUIRED_AUTHORIZATION_EVIDENCE[0]).write_text("[]\n", encoding="utf-8")

    with pytest.raises(AuthorizationEvidenceError, match="must be an object"):
        build_authorization_evidence_manifest(root, source_revision=REVISION)
