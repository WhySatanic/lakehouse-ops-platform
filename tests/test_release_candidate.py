from __future__ import annotations

import hashlib
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
