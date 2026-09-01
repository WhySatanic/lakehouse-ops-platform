from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lakehouse_ops.metadata_db_recovery import validate_metadata_db_recovery_report
from lakehouse_ops.metastore_recovery import validate_metastore_recovery_report
from lakehouse_ops.trino_worker_recovery import validate_trino_worker_recovery_report


class ReleaseReadinessError(RuntimeError):
    pass


REQUIRED_EVIDENCE = {
    "core_metadata",
    "file_authorization",
    "hive_metastore_recovery",
    "metadata_db_recovery",
    "trino_worker_recovery",
    "platform_slos",
    "ranger_authorization",
    "clickhouse_serving",
}

EXPECTED_AUTHORIZATION_OUTCOMES = {
    "platform_admin_reads_bronze": "allowed",
    "data_engineer_reads_bronze": "allowed",
    "analytics_engineer_silver_row_visibility": "allowed",
    "analytics_engineer_checksum_visibility": "allowed",
    "platform_admin_checksum_is_visible": "allowed",
    "operator_reads_system": "allowed",
    "analytics_engineer_cannot_read_bronze": "denied",
    "analyst_cannot_read_silver": "denied",
    "service_ingest_cannot_read_silver": "denied",
    "unknown_user_cannot_read_lakehouse": "denied",
    "unknown_user_cannot_read_system": "denied",
    "data_engineer_cannot_create_schema": "denied",
}


def verify_release_readiness(
    contract_path: Path,
    evidence_root: Path,
    *,
    source_revision: str,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    contract = _load_object(contract_path, "readiness contract")
    if contract.get("schema_version") != "1.0":
        raise ReleaseReadinessError("unsupported readiness contract schema_version")
    target_release = contract.get("target_release")
    if target_release != "1.0.0":
        raise ReleaseReadinessError("readiness contract must target release 1.0.0")
    entries = contract.get("evidence")
    if not isinstance(entries, list) or not entries:
        raise ReleaseReadinessError("readiness contract evidence must be a non-empty array")
    if not source_revision.strip():
        raise ReleaseReadinessError("source revision must be non-empty")

    reports: dict[str, dict[str, Any]] = {}
    verified: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ReleaseReadinessError("readiness evidence entry must be an object")
        key = entry.get("key")
        relative_path = entry.get("path")
        validator = entry.get("validator")
        if not all(isinstance(value, str) and value for value in (key, relative_path, validator)):
            raise ReleaseReadinessError("readiness evidence entry fields must be non-empty strings")
        if key in seen:
            raise ReleaseReadinessError(f"duplicate readiness evidence key: {key}")
        seen.add(key)
        path = _resolve_evidence_path(evidence_root, relative_path)
        report = _load_object(path, key)
        _validate_report(validator, report)
        reports[key] = report
        verified.append(
            {
                "key": key,
                "path": relative_path,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "validator": validator,
            }
        )

    if seen != REQUIRED_EVIDENCE:
        missing = sorted(REQUIRED_EVIDENCE - seen)
        unexpected = sorted(seen - REQUIRED_EVIDENCE)
        raise ReleaseReadinessError(
            f"readiness evidence coverage mismatch; missing={missing}, unexpected={unexpected}"
        )

    snapshot_ids = _snapshot_ids(reports)
    if len(set(snapshot_ids.values())) != 1:
        raise ReleaseReadinessError(f"core snapshot invariant failed: {snapshot_ids}")
    row_counts = _row_counts(reports)
    if set(row_counts.values()) != {2}:
        raise ReleaseReadinessError(f"core row-count invariant failed: {row_counts}")

    now = clock or (lambda: datetime.now(UTC))
    return {
        "schema_version": "1.0",
        "status": "ready",
        "target_release": target_release,
        "source_revision": source_revision,
        "generated_at": now().astimezone(UTC).isoformat(),
        "contract_sha256": hashlib.sha256(contract_path.read_bytes()).hexdigest(),
        "evidence": verified,
        "invariants": {
            "core_snapshot_id": next(iter(snapshot_ids.values())),
            "core_row_count": 2,
            "snapshot_consistent_across_core_and_recovery": True,
            "row_count_consistent_across_core_and_recovery": True,
        },
    }


def write_attestation(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _resolve_evidence_path(root: Path, relative_path: str) -> Path:
    if Path(relative_path).is_absolute() or ".." in Path(relative_path).parts:
        raise ReleaseReadinessError(f"evidence path must remain below root: {relative_path}")
    path = (root / relative_path).resolve()
    root = root.resolve()
    if root not in path.parents:
        raise ReleaseReadinessError(f"evidence path escapes root: {relative_path}")
    return path


def _load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReleaseReadinessError(f"cannot load {label}: {error}") from error
    if not isinstance(value, dict):
        raise ReleaseReadinessError(f"{label} must be a JSON object")
    return value


def _validate_report(name: str, report: dict[str, Any]) -> None:
    validators = {
        "iceberg_metadata": _validate_iceberg_metadata,
        "trino_file_authorization": lambda value: _validate_authorization(value, "file"),
        "trino_ranger_authorization": lambda value: _validate_authorization(value, "ranger"),
        "hive_metastore_recovery": validate_metastore_recovery_report,
        "metadata_db_recovery": validate_metadata_db_recovery_report,
        "trino_worker_recovery": validate_trino_worker_recovery_report,
        "platform_slos": _validate_platform_slos,
        "clickhouse_serving": _validate_clickhouse_serving,
    }
    try:
        validator = validators[name]
    except KeyError as error:
        raise ReleaseReadinessError(f"unsupported readiness validator: {name}") from error
    try:
        validator(report)
    except (KeyError, TypeError, ValueError, RuntimeError) as error:
        raise ReleaseReadinessError(f"{name} evidence is invalid: {error}") from error


def _validate_iceberg_metadata(report: dict[str, Any]) -> None:
    if report.get("schema_version") != "1.0" or report.get("status") != "ready":
        raise ValueError("metadata report is not ready schema 1.0 evidence")
    if report.get("table") != "lakehouse.silver.weather_hourly":
        raise ValueError("metadata report targets the wrong table")
    if report["files"]["records"] != 2 or report["files"]["count"] < 1:
        raise ValueError("metadata report has an invalid file or row count")
    snapshot_id = report["snapshots"]["current_id"]
    if not isinstance(snapshot_id, str) or not snapshot_id.isdigit():
        raise ValueError("metadata report has an invalid current snapshot")


def _validate_authorization(report: dict[str, Any], mode: str) -> None:
    if report.get("schema_version") != "1.0" or report.get("status") != "succeeded":
        raise ValueError("authorization report is not succeeded schema 1.0 evidence")
    policy = report.get("policy", {})
    if (
        policy.get("engine") != "trino"
        or policy.get("mode") != mode
        or policy.get("default") != "deny"
        or policy.get("authentication_enforced") is not False
    ):
        raise ValueError(f"authorization report does not prove deny-by-default {mode} mode")
    outcomes = {
        case.get("id"): case.get("result")
        for case in report.get("cases", [])
        if isinstance(case, dict)
    }
    if outcomes != EXPECTED_AUTHORIZATION_OUTCOMES:
        raise ValueError("authorization report does not cover the declared allow and deny matrix")
    transformations = report.get("transformations", {})
    expected = {
        "analytics_engineer_visible_rows": 1 if mode == "ranger" else 2,
        "analytics_engineer_visible_checksums": 0 if mode == "ranger" else 2,
        "platform_admin_visible_checksums": 2,
    }
    if transformations != expected:
        raise ValueError(f"authorization transformations do not match {mode} contract")


def _validate_platform_slos(report: dict[str, Any]) -> None:
    if report.get("schema_version") != "1.0" or report.get("status") != "ready":
        raise ValueError("platform SLO report is not ready schema 1.0 evidence")
    objectives = report.get("objectives")
    if not isinstance(objectives, dict) or len(objectives) != 5:
        raise ValueError("platform SLO report does not cover five objectives")
    if not all(item.get("met") is True for item in objectives.values()):
        raise ValueError("platform SLO report contains an unmet objective")


def _validate_clickhouse_serving(report: dict[str, Any]) -> None:
    expected = {
        "schema_version": "1.0",
        "status": "ready",
        "engine": "clickhouse",
        "mode": "direct_iceberg_s3",
        "silver_rows": 2,
        "reject_rows": 1,
        "duplicate_keys": 0,
        "latest_survivor": 1,
    }
    if any(report.get(key) != value for key, value in expected.items()):
        raise ValueError("ClickHouse report does not match the serving contract")


def _snapshot_ids(reports: dict[str, dict[str, Any]]) -> dict[str, str]:
    return {
        "core": str(reports["core_metadata"]["snapshots"]["current_id"]),
        "hive_metastore": str(reports["hive_metastore_recovery"]["recovery"]["snapshot_id"]),
        "metadata_db": str(reports["metadata_db_recovery"]["recovery"]["trino"]["snapshot_id"]),
        "trino_worker": str(reports["trino_worker_recovery"]["restored"]["snapshot_id"]),
    }


def _row_counts(reports: dict[str, dict[str, Any]]) -> dict[str, int]:
    return {
        "core": int(reports["core_metadata"]["files"]["records"]),
        "hive_metastore": int(reports["hive_metastore_recovery"]["recovery"]["row_count"]),
        "metadata_db": int(reports["metadata_db_recovery"]["recovery"]["trino"]["row_count"]),
        "trino_worker": int(reports["trino_worker_recovery"]["restored"]["row_count"]),
    }
