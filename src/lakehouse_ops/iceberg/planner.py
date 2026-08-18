from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any


class PlanningContractError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class MaintenancePolicy:
    target_file_size_bytes: int = 128 * 1024 * 1024
    small_file_ratio: float = 0.5
    min_data_files: int = 4
    min_manifest_count: int = 8
    max_manifests_per_data_file: float = 2.0
    snapshot_retention_hours: int = 168
    min_snapshots_to_keep: int = 3
    max_snapshots_to_expire: int = 50
    orphan_inspection_enabled: bool = False
    orphan_retention_hours: int = 168
    max_orphan_files: int = 1000

    def __post_init__(self) -> None:
        if self.target_file_size_bytes <= 0:
            raise ValueError("target_file_size_bytes must be positive")
        if not 0 < self.small_file_ratio <= 1:
            raise ValueError("small_file_ratio must be between zero and one")
        if self.min_data_files <= 0:
            raise ValueError("min_data_files must be positive")
        if self.min_manifest_count <= 0:
            raise ValueError("min_manifest_count must be positive")
        if self.max_manifests_per_data_file <= 0:
            raise ValueError("max_manifests_per_data_file must be positive")
        if self.snapshot_retention_hours < 0:
            raise ValueError("snapshot_retention_hours must be non-negative")
        if self.min_snapshots_to_keep < 2:
            raise ValueError("min_snapshots_to_keep must be at least two")
        if self.max_snapshots_to_expire <= 0:
            raise ValueError("max_snapshots_to_expire must be positive")
        if self.orphan_retention_hours < 72:
            raise ValueError("orphan_retention_hours must be at least 72")
        if self.max_orphan_files <= 0:
            raise ValueError("max_orphan_files must be positive")


@dataclass(frozen=True, slots=True)
class MaintenancePlan:
    schema_version: str
    plan_id: str
    status: str
    table: str
    source: dict[str, Any]
    policy: dict[str, Any]
    checks: tuple[dict[str, Any], ...]
    actions: tuple[dict[str, Any], ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class IcebergMaintenancePlanner:
    def __init__(self, policy: MaintenancePolicy | None = None) -> None:
        self._policy = policy or MaintenancePolicy()

    def plan(self, report: dict[str, Any]) -> MaintenancePlan:
        source = _validate_report(report)
        files = _object(report, "files")
        manifests = _object(report, "manifests")
        file_count = _integer(files, "count")
        delete_file_count = _integer(files, "delete_file_count")
        data_file_count = file_count - delete_file_count
        if data_file_count < 0:
            raise PlanningContractError("files.delete_file_count cannot exceed files.count")

        checks: list[dict[str, Any]] = []
        actions: list[dict[str, Any]] = []
        self._check_data_files(
            source,
            files,
            data_file_count,
            delete_file_count,
            checks,
            actions,
        )
        self._check_manifests(
            source,
            manifests,
            data_file_count,
            checks,
            actions,
        )
        self._check_snapshots(report, source, checks, actions)
        if self._policy.orphan_inspection_enabled:
            self._check_orphan_files(source, checks, actions)

        status = "maintenance_recommended" if actions else "healthy"
        if not actions and any(check["outcome"] == "deferred" for check in checks):
            status = "review_required"
        policy = asdict(self._policy)
        plan_seed = {
            "schema_version": "1.0",
            "status": status,
            "table": source["table"],
            "source": source,
            "policy": policy,
            "checks": checks,
            "actions": actions,
        }
        return MaintenancePlan(
            schema_version="1.0",
            plan_id=_identifier("plan", plan_seed),
            status=status,
            table=source["table"],
            source=source,
            policy=policy,
            checks=tuple(checks),
            actions=tuple(actions),
        )

    def _check_data_files(
        self,
        source: dict[str, Any],
        files: dict[str, Any],
        data_file_count: int,
        delete_file_count: int,
        checks: list[dict[str, Any]],
        actions: list[dict[str, Any]],
    ) -> None:
        total_size_bytes = _integer(files, "total_size_bytes")
        threshold = int(
            self._policy.target_file_size_bytes * self._policy.small_file_ratio
        )
        average_size_bytes = total_size_bytes // data_file_count if data_file_count else 0
        observed = {
            "data_file_count": data_file_count,
            "delete_file_count": delete_file_count,
            "average_size_bytes": average_size_bytes,
        }
        limits = {
            "min_data_files": self._policy.min_data_files,
            "average_size_below_bytes": threshold,
        }
        if delete_file_count:
            checks.append(
                _check(
                    "small_data_files",
                    "deferred",
                    observed,
                    limits,
                    "File-size aggregates include delete files; inspect content-specific metrics.",
                )
            )
            return
        if data_file_count >= self._policy.min_data_files and average_size_bytes < threshold:
            reason = (
                f"{data_file_count} data files average {average_size_bytes} bytes, "
                f"below the {threshold}-byte policy threshold."
            )
            checks.append(_check("small_data_files", "recommend", observed, limits, reason))
            actions.append(
                _action(
                    source,
                    "rewrite_data_files",
                    "small_files",
                    reason,
                    {"target_file_size_bytes": self._policy.target_file_size_bytes},
                )
            )
            return
        checks.append(
            _check(
                "small_data_files",
                "healthy",
                observed,
                limits,
                "Data-file count or average size does not cross the compaction threshold.",
            )
        )

    def _check_manifests(
        self,
        source: dict[str, Any],
        manifests: dict[str, Any],
        data_file_count: int,
        checks: list[dict[str, Any]],
        actions: list[dict[str, Any]],
    ) -> None:
        manifest_count = _integer(manifests, "count")
        ratio = manifest_count / max(data_file_count, 1)
        observed = {
            "manifest_count": manifest_count,
            "manifests_per_data_file": round(ratio, 4),
        }
        limits = {
            "min_manifest_count": self._policy.min_manifest_count,
            "manifests_per_data_file_at_least": (
                self._policy.max_manifests_per_data_file
            ),
        }
        if (
            manifest_count >= self._policy.min_manifest_count
            and ratio >= self._policy.max_manifests_per_data_file
        ):
            reason = (
                f"{manifest_count} manifests for {data_file_count} data files gives a "
                f"{ratio:.4f} manifest-to-file ratio."
            )
            checks.append(_check("manifest_density", "recommend", observed, limits, reason))
            actions.append(
                _action(source, "rewrite_manifests", "dense_manifests", reason, {})
            )
            return
        checks.append(
            _check(
                "manifest_density",
                "healthy",
                observed,
                limits,
                "Manifest count and density remain within policy bounds.",
            )
        )

    def _check_snapshots(
        self,
        report: dict[str, Any],
        source: dict[str, Any],
        checks: list[dict[str, Any]],
        actions: list[dict[str, Any]],
    ) -> None:
        snapshots = _object(report, "snapshots")
        history = snapshots.get("history")
        references = report.get("references")
        limits = {
            "retention_hours": self._policy.snapshot_retention_hours,
            "min_snapshots_to_keep": self._policy.min_snapshots_to_keep,
            "max_snapshots_to_expire": self._policy.max_snapshots_to_expire,
        }
        if not isinstance(history, list) or not isinstance(references, list):
            checks.append(
                _check(
                    "snapshot_retention",
                    "deferred",
                    {"history_available": False},
                    limits,
                    "Collect snapshot history and references before planning expiration.",
                )
            )
            return
        non_main_refs = [
            ref.get("name")
            for ref in references
            if isinstance(ref, dict) and ref.get("name") != "main"
        ]
        if non_main_refs:
            checks.append(
                _check(
                    "snapshot_retention",
                    "deferred",
                    {"non_main_references": sorted(str(name) for name in non_main_refs)},
                    limits,
                    "Snapshot expiration is deferred while non-main references exist.",
                )
            )
            return
        normalized = [_snapshot_entry(item) for item in history]
        ids = [entry["snapshot_id"] for entry in normalized]
        if len(ids) != len(set(ids)):
            raise PlanningContractError("snapshot history contains duplicate IDs")
        normalized.sort(key=lambda entry: entry["committed_at"])
        protected = {
            entry["snapshot_id"]
            for entry in normalized[-self._policy.min_snapshots_to_keep :]
        }
        cutoff = _timestamp(source["collected_at"]) - timedelta(
            hours=self._policy.snapshot_retention_hours
        )
        candidates = [
            entry["snapshot_id"]
            for entry in normalized
            if entry["committed_at"] < cutoff and entry["snapshot_id"] not in protected
        ]
        observed = {
            "snapshot_count": len(normalized),
            "eligible_snapshot_count": len(candidates),
            "cutoff": cutoff.isoformat(),
        }
        if not candidates:
            checks.append(
                _check(
                    "snapshot_retention",
                    "healthy",
                    observed,
                    limits,
                    "No snapshots are eligible after applying age and minimum-count retention.",
                )
            )
            return
        selected = candidates[: self._policy.max_snapshots_to_expire]
        reason = (
            f"{len(candidates)} snapshots are older than {cutoff.isoformat()} after "
            f"preserving the latest {self._policy.min_snapshots_to_keep}."
        )
        checks.append(_check("snapshot_retention", "recommend", observed, limits, reason))
        actions.append(
            _action(
                source,
                "expire_snapshots",
                "retention_window",
                reason,
                {"snapshot_ids": selected},
                {
                    "max_snapshots_to_expire": self._policy.max_snapshots_to_expire,
                    "expected_history_snapshot_ids": ids,
                },
            )
        )

    def _check_orphan_files(
        self,
        source: dict[str, Any],
        checks: list[dict[str, Any]],
        actions: list[dict[str, Any]],
    ) -> None:
        cutoff = _timestamp(source["collected_at"]) - timedelta(
            hours=self._policy.orphan_retention_hours
        )
        limits = {
            "retention_hours": self._policy.orphan_retention_hours,
            "max_orphan_files": self._policy.max_orphan_files,
        }
        reason = (
            "Inspect unreferenced files older than "
            f"{cutoff.isoformat()} before approving any deletion."
        )
        checks.append(
            _check(
                "orphan_file_inventory",
                "inspect",
                {"cutoff": cutoff.isoformat()},
                limits,
                reason,
            )
        )
        actions.append(
            _action(
                source,
                "inspect_orphan_files",
                "scheduled_inventory",
                reason,
                {"older_than": cutoff.isoformat()},
                {"max_orphan_files": self._policy.max_orphan_files},
            )
        )


def _validate_report(report: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(report, dict):
        raise PlanningContractError("metadata report must be a JSON object")
    if report.get("schema_version") != "1.0":
        raise PlanningContractError("unsupported metadata schema_version")
    if report.get("status") != "ready":
        raise PlanningContractError("metadata report status must be ready")
    table = report.get("table")
    collected_at = report.get("collected_at")
    if not isinstance(table, str) or not table:
        raise PlanningContractError("table must be a non-empty string")
    if not isinstance(collected_at, str) or not collected_at:
        raise PlanningContractError("collected_at must be a non-empty string")
    snapshots = _object(report, "snapshots")
    current_snapshot_id = snapshots.get("current_id")
    if not isinstance(current_snapshot_id, str) or not current_snapshot_id:
        raise PlanningContractError("snapshots.current_id must be a non-empty string")
    _object(report, "files")
    _object(report, "manifests")
    return {
        "metadata_schema_version": report["schema_version"],
        "collected_at": collected_at,
        "current_snapshot_id": current_snapshot_id,
        "table": table,
    }


def _object(value: dict[str, Any], key: str) -> dict[str, Any]:
    item = value.get(key)
    if not isinstance(item, dict):
        raise PlanningContractError(f"{key} must be an object")
    return item


def _integer(value: dict[str, Any], key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int) or item < 0:
        raise PlanningContractError(f"{key} must be a non-negative integer")
    return item


def _snapshot_entry(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PlanningContractError("snapshot history entries must be objects")
    snapshot_id = value.get("snapshot_id")
    committed_at = value.get("committed_at")
    if not isinstance(snapshot_id, str) or not snapshot_id.isdigit():
        raise PlanningContractError("snapshot history IDs must be decimal strings")
    if not isinstance(committed_at, str):
        raise PlanningContractError("snapshot committed_at must be a string")
    return {"snapshot_id": snapshot_id, "committed_at": _timestamp(committed_at)}


def _timestamp(value: str) -> datetime:
    normalized = value.replace(" UTC", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise PlanningContractError(f"invalid timestamp: {value}") from error
    if parsed.tzinfo is None:
        raise PlanningContractError("timestamps must include a timezone")
    return parsed.astimezone(UTC)


def _check(
    rule: str,
    outcome: str,
    observed: dict[str, Any],
    threshold: dict[str, Any],
    reason: str,
) -> dict[str, Any]:
    return {
        "rule": rule,
        "outcome": outcome,
        "observed": observed,
        "threshold": threshold,
        "reason": reason,
    }


def _action(
    source: dict[str, Any],
    action_type: str,
    reason_code: str,
    reason: str,
    parameters: dict[str, Any],
    additional_safety: dict[str, Any] | None = None,
) -> dict[str, Any]:
    bounded_resource = {
        "rewrite_data_files": {"max_files_to_rewrite": 1000},
        "rewrite_manifests": {"max_manifests_to_rewrite": 1000},
        "expire_snapshots": {},
        "inspect_orphan_files": {},
    }[action_type]
    action = {
        "action_type": action_type,
        "reason_code": reason_code,
        "reason": reason,
        "parameters": parameters,
        "safety_bounds": {
            "dry_run_required": True,
            "expected_snapshot_id": source["current_snapshot_id"],
            "max_concurrent_jobs": 1,
            **bounded_resource,
            **(additional_safety or {}),
        },
    }
    return {"action_id": _identifier("action", action | {"source": source}), **action}


def _identifier(prefix: str, value: dict[str, Any]) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"
