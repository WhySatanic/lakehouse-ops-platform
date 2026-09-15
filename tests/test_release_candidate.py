from __future__ import annotations

import hashlib
import io
import json
import tarfile
from pathlib import Path

import pytest

import lakehouse_ops.release_candidate as candidate
from lakehouse_ops.release_candidate import ReleaseCandidateError, build_release_candidate


def test_build_release_candidate_is_deterministic_and_complete(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    inputs = _write_inputs(tmp_path)
    observed: dict[str, object] = {}

    def fake_validate(report: dict[str, object], plan: dict[str, object]) -> None:
        observed.update(report=report, plan=plan)

    monkeypatch.setattr(candidate, "validate_upgrade_report", fake_validate)
    first = tmp_path / "first.tar.gz"
    second = tmp_path / "second.tar.gz"

    report = build_release_candidate(**inputs, output_path=first)
    build_release_candidate(**inputs, output_path=second)

    assert report["status"] == "ready"
    assert report["entries"] == 7
    assert first.read_bytes() == second.read_bytes()
    assert report["bundle_sha256"] == hashlib.sha256(first.read_bytes()).hexdigest()
    with tarfile.open(first, "r:gz") as archive:
        names = archive.getnames()
        manifest = json.load(archive.extractfile("manifest.json"))  # type: ignore[arg-type]
    assert names == sorted(names)
    assert "evidence/lakehouse-evidence/core.json" in names
    assert "upgrade/trino-upgrade-rehearsal.json" in names
    assert manifest["source_revision"] == "a" * 40
    assert observed["report"] == {"status": "ready"}
    assert observed["plan"]["source"]["version"] == "482"  # type: ignore[index]


def test_build_release_candidate_rejects_report_schema_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    inputs = _write_inputs(tmp_path)
    monkeypatch.setattr(candidate, "validate_upgrade_report", lambda report, plan: None)
    source = Path(
        "config/control-plane/schemas/release-candidate-bundle-report.schema.json"
    )
    schema = json.loads(source.read_text(encoding="utf-8"))
    schema["properties"]["status"] = {"const": "blocked"}
    candidate_schema = tmp_path / "release-candidate.schema.json"
    candidate_schema.write_text(json.dumps(schema), encoding="utf-8")

    with pytest.raises(ReleaseCandidateError, match="report schema validation failed"):
        build_release_candidate(
            **inputs,
            output_path=tmp_path / "bundle.tar.gz",
            schema_path=candidate_schema,
        )


def test_build_release_candidate_rejects_tampered_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    inputs = _write_inputs(tmp_path)
    monkeypatch.setattr(candidate, "validate_upgrade_report", lambda report, plan: None)
    evidence = inputs["evidence_root"] / "lakehouse-evidence" / "core.json"
    evidence.write_text('{"tampered": true}', encoding="utf-8")

    with pytest.raises(ReleaseCandidateError, match="evidence digest changed"):
        build_release_candidate(**inputs, output_path=tmp_path / "bundle.tar.gz")


def test_build_release_candidate_rejects_revision_mismatch(tmp_path: Path) -> None:
    inputs = _write_inputs(tmp_path)
    inputs["source_revision"] = "b" * 40

    with pytest.raises(ReleaseCandidateError, match="source revision"):
        build_release_candidate(**inputs, output_path=tmp_path / "bundle.tar.gz")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("status", "failed", "attestation is not ready"),
        ("evidence", [], "non-empty array"),
        ("evidence", [None], "entry must be an object"),
        ("evidence", [{}], "fields are invalid"),
        (
            "evidence",
            [{"path": "../outside.json", "sha256": "a" * 64}],
            "remain below root",
        ),
    ],
)
def test_build_release_candidate_rejects_malformed_attestation(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    inputs = _write_inputs(tmp_path)
    attestation = inputs["attestation_path"]
    assert isinstance(attestation, Path)
    content = json.loads(attestation.read_text(encoding="utf-8"))
    content[field] = value
    attestation.write_text(json.dumps(content), encoding="utf-8")

    with pytest.raises(ReleaseCandidateError, match=message):
        build_release_candidate(**inputs, output_path=tmp_path / "bundle.tar.gz")


def test_build_release_candidate_rejects_contract_digest_change(tmp_path: Path) -> None:
    inputs = _write_inputs(tmp_path)
    contract = inputs["readiness_contract_path"]
    assert isinstance(contract, Path)
    contract.write_text('{"changed": true}', encoding="utf-8")

    with pytest.raises(ReleaseCandidateError, match="contract digest"):
        build_release_candidate(**inputs, output_path=tmp_path / "bundle.tar.gz")


def test_build_release_candidate_rejects_upgrade_validation_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    inputs = _write_inputs(tmp_path)

    def fail_validation(report: object, plan: object) -> None:
        raise ValueError("rollback failed")

    monkeypatch.setattr(candidate, "validate_upgrade_report", fail_validation)

    with pytest.raises(ReleaseCandidateError, match="rollback evidence is invalid"):
        build_release_candidate(**inputs, output_path=tmp_path / "bundle.tar.gz")


def test_build_release_candidate_rejects_invalid_attestation_json(tmp_path: Path) -> None:
    inputs = _write_inputs(tmp_path)
    attestation = inputs["attestation_path"]
    assert isinstance(attestation, Path)
    attestation.write_text("{", encoding="utf-8")

    with pytest.raises(ReleaseCandidateError, match="cannot load"):
        build_release_candidate(**inputs, output_path=tmp_path / "bundle.tar.gz")


def test_build_release_candidate_requires_source_revision(tmp_path: Path) -> None:
    inputs = _write_inputs(tmp_path)
    inputs["source_revision"] = " "

    with pytest.raises(ReleaseCandidateError, match="source revision"):
        build_release_candidate(**inputs, output_path=tmp_path / "bundle.tar.gz")


def test_verify_release_candidate_checks_downloaded_pair(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bundle, report_path, report = _build_pair(monkeypatch, tmp_path)

    verified = candidate.verify_release_candidate(
        bundle_path=bundle,
        report_path=report_path,
        expected_source_revision="a" * 40,
    )

    assert verified == {
        "schema_version": "1.0",
        "status": "verified",
        "target_release": "1.0.0",
        "source_revision": "a" * 40,
        "entries": 7,
        "bundle_sha256": report["bundle_sha256"],
        "report_sha256": _digest(report_path),
        "manifest_sha256": verified["manifest_sha256"],
    }
    assert len(verified["manifest_sha256"]) == 64


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("report", "report schema validation failed"),
        ("bundle", "bundle digest does not match report"),
        ("revision", "source revision does not match expected revision"),
        ("member", "archive member digest does not match manifest"),
    ],
)
def test_verify_release_candidate_rejects_tampering(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    bundle, report_path, report = _build_pair(monkeypatch, tmp_path)
    expected_revision = "a" * 40
    if mutation == "report":
        report["status"] = "failed"
        _write_json(report_path, report)
    elif mutation == "bundle":
        bundle.write_bytes(bundle.read_bytes() + b"tampered")
    elif mutation == "revision":
        expected_revision = "b" * 40
    else:
        entries = _read_archive(bundle)
        entries["evidence/lakehouse-evidence/core.json"] = b'{"tampered":true}'
        candidate._write_deterministic_archive(bundle, entries)
        report["bundle_sha256"] = _digest(bundle)
        _write_json(report_path, report)

    with pytest.raises(ReleaseCandidateError, match=message):
        candidate.verify_release_candidate(
            bundle_path=bundle,
            report_path=report_path,
            expected_source_revision=expected_revision,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("unexpected", "archive membership does not match manifest"),
        ("unsafe", "archive member path is unsafe"),
        ("duplicate", "duplicate archive member"),
        ("symlink", "archive member must be a regular file"),
    ],
)
def test_verify_release_candidate_rejects_unsafe_archive_structure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    bundle, report_path, report = _build_pair(monkeypatch, tmp_path)
    members = [(name, content, tarfile.REGTYPE) for name, content in _read_archive(bundle).items()]
    if mutation == "unexpected":
        members.append(("extra.json", b"{}", tarfile.REGTYPE))
    elif mutation == "unsafe":
        members.append(("../escape.json", b"{}", tarfile.REGTYPE))
    elif mutation == "duplicate":
        members.append((members[0][0], members[0][1], tarfile.REGTYPE))
    else:
        members.append(("link", b"", tarfile.SYMTYPE))
    _write_archive(bundle, members)
    report["bundle_sha256"] = _digest(bundle)
    report["entries"] = len(members)
    _write_json(report_path, report)

    with pytest.raises(ReleaseCandidateError, match=message):
        candidate.verify_release_candidate(
            bundle_path=bundle,
            report_path=report_path,
            expected_source_revision="a" * 40,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("missing", "archive manifest is missing"),
        ("json", "cannot load archive manifest"),
        ("object", "archive manifest must be a JSON object"),
        ("fields", "archive manifest fields are invalid"),
        ("identity", "archive manifest identity is invalid"),
        ("empty", "manifest entries must be a non-empty array"),
        ("entry-fields", "manifest entry fields are invalid"),
        ("entry-values", "manifest entry values are invalid"),
        ("entry-duplicate", "duplicate archive manifest entry"),
        ("entry-digest", "archive manifest digest is invalid"),
    ],
)
def test_verify_release_candidate_rejects_invalid_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    bundle, report_path, report = _build_pair(monkeypatch, tmp_path)
    entries = _read_archive(bundle)
    if mutation == "missing":
        entries.pop("manifest.json")
    elif mutation == "json":
        entries["manifest.json"] = b"{"
    elif mutation == "object":
        entries["manifest.json"] = b"[]"
    else:
        manifest = json.loads(entries["manifest.json"])
        if mutation == "fields":
            manifest["extra"] = True
        elif mutation == "identity":
            manifest["source_revision"] = "b" * 40
        elif mutation == "empty":
            manifest["entries"] = []
        elif mutation == "entry-fields":
            manifest["entries"][0]["extra"] = True
        elif mutation == "entry-values":
            manifest["entries"][0]["path"] = 1
        elif mutation == "entry-duplicate":
            manifest["entries"].append(manifest["entries"][0])
        else:
            manifest["entries"][0]["sha256"] = "invalid"
        entries["manifest.json"] = json.dumps(manifest).encode()
    candidate._write_deterministic_archive(bundle, entries)
    report["bundle_sha256"] = _digest(bundle)
    report["entries"] = len(entries)
    _write_json(report_path, report)

    with pytest.raises(ReleaseCandidateError, match=message):
        candidate.verify_release_candidate(
            bundle_path=bundle,
            report_path=report_path,
            expected_source_revision="a" * 40,
        )


def test_verify_release_candidate_rejects_report_entry_count(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bundle, report_path, report = _build_pair(monkeypatch, tmp_path)
    report["entries"] = 6
    _write_json(report_path, report)

    with pytest.raises(ReleaseCandidateError, match="entry count does not match"):
        candidate.verify_release_candidate(
            bundle_path=bundle,
            report_path=report_path,
            expected_source_revision="a" * 40,
        )


def test_verify_release_candidate_enforces_archive_bounds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bundle, report_path, _ = _build_pair(monkeypatch, tmp_path)
    monkeypatch.setattr(candidate, "MAX_ARCHIVE_BYTES", 1)

    with pytest.raises(ReleaseCandidateError, match="expands beyond the size limit"):
        candidate.verify_release_candidate(
            bundle_path=bundle,
            report_path=report_path,
            expected_source_revision="a" * 40,
        )


def test_verify_release_candidate_rejects_output_schema_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bundle, report_path, _ = _build_pair(monkeypatch, tmp_path)
    schema = json.loads(candidate.DEFAULT_VERIFICATION_SCHEMA.read_text(encoding="utf-8"))
    schema["properties"]["status"] = {"const": "blocked"}
    schema_path = tmp_path / "verification.schema.json"
    _write_json(schema_path, schema)

    with pytest.raises(ReleaseCandidateError, match="report schema validation failed"):
        candidate.verify_release_candidate(
            bundle_path=bundle,
            report_path=report_path,
            expected_source_revision="a" * 40,
            verification_schema_path=schema_path,
        )


def test_verify_release_candidate_requires_expected_revision(tmp_path: Path) -> None:
    with pytest.raises(ReleaseCandidateError, match="expected source revision"):
        candidate.verify_release_candidate(
            bundle_path=tmp_path / "bundle.tar.gz",
            report_path=tmp_path / "report.json",
            expected_source_revision=" ",
        )


def _build_pair(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> tuple[Path, Path, dict[str, object]]:
    inputs = _write_inputs(tmp_path)
    monkeypatch.setattr(candidate, "validate_upgrade_report", lambda report, plan: None)
    bundle = tmp_path / "bundle.tar.gz"
    report = build_release_candidate(**inputs, output_path=bundle)
    report_path = tmp_path / "release-candidate.json"
    _write_json(report_path, report)
    return bundle, report_path, report


def _read_archive(path: Path) -> dict[str, bytes]:
    with tarfile.open(path, "r:gz") as archive:
        return {
            member.name: archive.extractfile(member).read()  # type: ignore[union-attr]
            for member in archive.getmembers()
        }


def _write_archive(path: Path, members: list[tuple[str, bytes, bytes]]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for name, content, member_type in members:
            info = tarfile.TarInfo(name)
            info.type = member_type
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def _write_inputs(tmp_path: Path) -> dict[str, object]:
    evidence_root = tmp_path / "evidence"
    evidence_path = evidence_root / "lakehouse-evidence" / "core.json"
    evidence_path.parent.mkdir(parents=True)
    evidence_path.write_text('{"status": "ready"}', encoding="utf-8")
    readiness_contract = tmp_path / "readiness-contract.json"
    readiness_contract.write_text('{"schema_version": "1.0"}', encoding="utf-8")
    control_plane_contract = tmp_path / "control-plane-contract.json"
    control_plane_contract.write_text('{"contract_version": "1.0.0"}', encoding="utf-8")
    upgrade_plan = tmp_path / "upgrade-plan.json"
    upgrade_plan.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "source": {"version": "482", "image": "trinodb/trino:482"},
                "target": {"version": "483", "image": "trinodb/trino:483"},
                "release_notes": "https://trino.io/docs/current/release/release-483.html",
            }
        ),
        encoding="utf-8",
    )
    upgrade_report = tmp_path / "upgrade-report.json"
    upgrade_report.write_text('{"status": "ready"}', encoding="utf-8")
    revision = "a" * 40
    attestation = tmp_path / "attestation.json"
    attestation.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "status": "ready",
                "target_release": "1.0.0",
                "source_revision": revision,
                "contract_sha256": _digest(readiness_contract),
                "evidence": [
                    {
                        "path": "lakehouse-evidence/core.json",
                        "sha256": _digest(evidence_path),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return {
        "evidence_root": evidence_root,
        "attestation_path": attestation,
        "readiness_contract_path": readiness_contract,
        "control_plane_contract_path": control_plane_contract,
        "upgrade_report_path": upgrade_report,
        "upgrade_plan_path": upgrade_plan,
        "source_revision": revision,
    }


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
