from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import lakehouse_ops.release_readiness as readiness
from lakehouse_ops.release_readiness import (
    ReleaseReadinessError,
    verify_release_readiness,
    write_attestation,
)


def authorization(mode: str) -> dict[str, Any]:
    cases = [
        {
            "id": case_id,
            "expectation": "deny" if result == "denied" else "allow",
            "result": result,
        }
        for case_id, result in readiness.EXPECTED_AUTHORIZATION_OUTCOMES.items()
    ]
    return {
        "schema_version": "1.0",
        "status": "succeeded",
        "policy": {
            "engine": "trino",
            "mode": mode,
            "default": "deny",
            "authentication_enforced": False,
        },
        "transformations": {
            "analytics_engineer_visible_rows": 1 if mode == "ranger" else 2,
            "analytics_engineer_visible_checksums": 0 if mode == "ranger" else 2,
            "platform_admin_visible_checksums": 2,
        },
        "cases": cases,
    }


def evidence_reports(snapshot_id: str = "42") -> dict[str, dict[str, Any]]:
    return {
        "core_metadata": {
            "schema_version": "1.0",
            "status": "ready",
            "table": "lakehouse.silver.weather_hourly",
            "files": {"records": 2, "count": 1},
            "snapshots": {"current_id": snapshot_id},
        },
        "file_authorization": authorization("file"),
        "hive_metastore_recovery": {
            "recovery": {"snapshot_id": snapshot_id, "row_count": 2}
        },
        "metadata_db_recovery": {
            "recovery": {"trino": {"snapshot_id": snapshot_id, "row_count": 2}}
        },
        "trino_worker_recovery": {
            "restored": {"snapshot_id": snapshot_id, "row_count": 2}
        },
        "platform_slos": {
            "schema_version": "1.0",
            "status": "ready",
            "objectives": {f"objective-{index}": {"met": True} for index in range(5)},
        },
        "ranger_authorization": authorization("ranger"),
        "clickhouse_serving": {
            "schema_version": "1.0",
            "status": "ready",
            "engine": "clickhouse",
            "mode": "direct_iceberg_s3",
            "silver_rows": 2,
            "reject_rows": 1,
            "duplicate_keys": 0,
            "latest_survivor": 1,
        },
    }


def write_bundle(tmp_path: Path, reports: dict[str, dict[str, Any]]) -> tuple[Path, Path]:
    root = tmp_path / "evidence"
    entries = []
    validators = {
        "core_metadata": "iceberg_metadata",
        "file_authorization": "trino_file_authorization",
        "hive_metastore_recovery": "hive_metastore_recovery",
        "metadata_db_recovery": "metadata_db_recovery",
        "trino_worker_recovery": "trino_worker_recovery",
        "platform_slos": "platform_slos",
        "ranger_authorization": "trino_ranger_authorization",
        "clickhouse_serving": "clickhouse_serving",
    }
    for key, report in reports.items():
        path = root / f"{key}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report), encoding="utf-8")
        entries.append({"key": key, "path": f"{key}.json", "validator": validators[key]})
    contract = tmp_path / "contract.json"
    contract.write_text(
        json.dumps(
            {"schema_version": "1.0", "target_release": "1.0.0", "evidence": entries}
        ),
        encoding="utf-8",
    )
    return contract, root


@pytest.fixture(autouse=True)
def stub_detailed_recovery_validators(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(readiness, "validate_metastore_recovery_report", lambda report: None)
    monkeypatch.setattr(readiness, "validate_metadata_db_recovery_report", lambda report: None)
    monkeypatch.setattr(readiness, "validate_trino_worker_recovery_report", lambda report: None)


def test_verify_release_readiness_attests_cross_profile_invariants(tmp_path: Path) -> None:
    contract, root = write_bundle(tmp_path, evidence_reports())

    report = verify_release_readiness(
        contract,
        root,
        source_revision="abc123",
        clock=lambda: datetime(2026, 9, 1, tzinfo=UTC),
    )

    assert report["status"] == "ready"
    assert report["target_release"] == "1.0.0"
    assert report["source_revision"] == "abc123"
    assert len(report["evidence"]) == 8
    assert all(len(item["sha256"]) == 64 for item in report["evidence"])
    assert report["invariants"] == {
        "core_snapshot_id": "42",
        "core_row_count": 2,
        "snapshot_consistent_across_core_and_recovery": True,
        "row_count_consistent_across_core_and_recovery": True,
    }

    output = tmp_path / "artifacts" / "attestation.json"
    write_attestation(report, output)
    assert json.loads(output.read_text(encoding="utf-8")) == report


def test_verify_release_readiness_rejects_cross_recovery_snapshot_drift(
    tmp_path: Path,
) -> None:
    reports = evidence_reports()
    reports["trino_worker_recovery"]["restored"]["snapshot_id"] = "99"
    contract, root = write_bundle(tmp_path, reports)

    with pytest.raises(ReleaseReadinessError, match="snapshot invariant"):
        verify_release_readiness(contract, root, source_revision="abc123")


def test_verify_release_readiness_rejects_path_escape(tmp_path: Path) -> None:
    contract, root = write_bundle(tmp_path, evidence_reports())
    value = json.loads(contract.read_text(encoding="utf-8"))
    value["evidence"][0]["path"] = "../outside.json"
    contract.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(ReleaseReadinessError, match="remain below root"):
        verify_release_readiness(contract, root, source_revision="abc123")


def test_verify_release_readiness_rejects_unknown_validator(tmp_path: Path) -> None:
    contract, root = write_bundle(tmp_path, evidence_reports())
    value = json.loads(contract.read_text(encoding="utf-8"))
    value["evidence"][0]["validator"] = "unknown"
    contract.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(ReleaseReadinessError, match="unsupported readiness validator"):
        verify_release_readiness(contract, root, source_revision="abc123")


def test_verify_release_readiness_rejects_unproven_ranger_transformations(
    tmp_path: Path,
) -> None:
    reports = evidence_reports()
    reports["ranger_authorization"]["transformations"][
        "analytics_engineer_visible_rows"
    ] = 2
    contract, root = write_bundle(tmp_path, reports)

    with pytest.raises(ReleaseReadinessError, match="transformations"):
        verify_release_readiness(contract, root, source_revision="abc123")


@pytest.mark.parametrize(
    "contract_value, revision, message",
    [
        (
            {"schema_version": "2.0", "target_release": "1.0.0", "evidence": []},
            "abc",
            "schema_version",
        ),
        (
            {"schema_version": "1.0", "target_release": "0.46.0", "evidence": []},
            "abc",
            "target release",
        ),
        (
            {"schema_version": "1.0", "target_release": "1.0.0", "evidence": []},
            "abc",
            "non-empty array",
        ),
        (
            {"schema_version": "1.0", "target_release": "1.0.0", "evidence": [{}]},
            "",
            "source revision",
        ),
    ],
)
def test_verify_release_readiness_rejects_invalid_contract_header(
    tmp_path: Path,
    contract_value: dict[str, Any],
    revision: str,
    message: str,
) -> None:
    contract = tmp_path / "contract.json"
    contract.write_text(json.dumps(contract_value), encoding="utf-8")

    with pytest.raises(ReleaseReadinessError, match=message):
        verify_release_readiness(contract, tmp_path / "evidence", source_revision=revision)
