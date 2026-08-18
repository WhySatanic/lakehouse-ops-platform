from __future__ import annotations

from typing import Any

import pytest

from lakehouse_ops.iceberg.executor import (
    ExecutionContractError,
    SparkMaintenanceExecutor,
)
from lakehouse_ops.iceberg.planner import IcebergMaintenancePlanner, MaintenancePolicy


class FakeSqlExecutor:
    def __init__(self, *, snapshot_id: str = "8750000000000000001") -> None:
        self.snapshot_id = snapshot_id
        self.file_count = 10
        self.record_count = 100
        self.manifest_count = 4
        self.queries: list[str] = []

    def query(self, sql: str) -> list[dict[str, Any]]:
        self.queries.append(sql)
        if ".snapshots" in sql:
            return [{"snapshot_id": self.snapshot_id}]
        if ".data_files" in sql:
            return [
                {
                    "data_file_count": self.file_count,
                    "record_count": self.record_count,
                }
            ]
        if ".manifests" in sql:
            return [{"manifest_count": self.manifest_count}]
        if "rewrite_data_files" in sql:
            self.snapshot_id = "8750000000000000002"
            self.file_count = 1
            return [
                {
                    "rewritten_data_files_count": 10,
                    "added_data_files_count": 1,
                    "rewritten_bytes_count": 104857600,
                    "failed_data_files_count": 0,
                }
            ]
        if "rewrite_manifests" in sql:
            self.snapshot_id = "8750000000000000002"
            self.manifest_count = 1
            return [
                {
                    "rewritten_manifests_count": 4,
                    "added_manifests_count": 1,
                }
            ]
        raise AssertionError(sql)


def maintenance_plan() -> dict[str, Any]:
    report = {
        "schema_version": "1.0",
        "status": "ready",
        "collected_at": "2026-08-18T13:30:00+00:00",
        "table": "lakehouse.silver.weather_hourly",
        "snapshots": {"current_id": "8750000000000000001"},
        "files": {
            "count": 10,
            "total_size_bytes": 100 * 1024 * 1024,
            "delete_file_count": 0,
        },
        "manifests": {"count": 2},
    }
    return IcebergMaintenancePlanner().plan(report).as_dict()


def manifest_plan() -> dict[str, Any]:
    report = {
        "schema_version": "1.0",
        "status": "ready",
        "collected_at": "2026-08-18T13:30:00+00:00",
        "table": "lakehouse.silver.weather_hourly",
        "snapshots": {"current_id": "8750000000000000001"},
        "files": {
            "count": 4,
            "total_size_bytes": 4 * 128 * 1024 * 1024,
            "delete_file_count": 0,
        },
        "manifests": {"count": 4},
    }
    policy = MaintenancePolicy(
        min_data_files=100,
        min_manifest_count=2,
        max_manifests_per_data_file=0.5,
    )
    return IcebergMaintenancePlanner(policy).plan(report).as_dict()


def test_dry_run_checks_snapshot_without_calling_procedure() -> None:
    plan = maintenance_plan()
    executor = FakeSqlExecutor()

    report = SparkMaintenanceExecutor(executor).run(
        plan, plan["actions"][0]["action_id"]
    )

    assert report.status == "dry_run"
    assert report.applied is False
    assert report.before == report.after
    assert all("CALL" not in sql for sql in executor.queries)


def test_apply_requires_matching_approvals() -> None:
    plan = maintenance_plan()
    action_id = plan["actions"][0]["action_id"]

    with pytest.raises(ExecutionContractError, match="approved plan ID"):
        SparkMaintenanceExecutor(FakeSqlExecutor()).run(
            plan,
            action_id,
            apply=True,
            approved_plan_id="wrong",
            approved_snapshot_id=plan["source"]["current_snapshot_id"],
        )


def test_apply_executes_bounded_rewrite_and_reconciles() -> None:
    plan = maintenance_plan()
    action_id = plan["actions"][0]["action_id"]
    executor = FakeSqlExecutor()

    report = SparkMaintenanceExecutor(executor).run(
        plan,
        action_id,
        apply=True,
        approved_plan_id=plan["plan_id"],
        approved_snapshot_id=plan["source"]["current_snapshot_id"],
    )

    assert report.status == "succeeded"
    assert report.before.data_file_count == 10
    assert report.after.data_file_count == 1
    assert report.before.record_count == report.after.record_count == 100
    procedure = next(sql for sql in executor.queries if "CALL" in sql)
    assert "'max-concurrent-file-group-rewrites', '1'" in procedure
    assert "'partial-progress.enabled', 'false'" in procedure
    assert "'max-files-to-rewrite', '1000'" in procedure


def test_stale_snapshot_is_rejected_before_procedure() -> None:
    plan = maintenance_plan()
    executor = FakeSqlExecutor(snapshot_id="new-snapshot")

    with pytest.raises(ExecutionContractError, match="current snapshot"):
        SparkMaintenanceExecutor(executor).run(
            plan, plan["actions"][0]["action_id"]
        )

    assert all("CALL" not in sql for sql in executor.queries)


def test_record_count_change_fails_reconciliation() -> None:
    class CorruptingExecutor(FakeSqlExecutor):
        def query(self, sql: str) -> list[dict[str, Any]]:
            result = super().query(sql)
            if "rewrite_data_files" in sql:
                self.record_count -= 1
            return result

    plan = maintenance_plan()
    report = SparkMaintenanceExecutor(CorruptingExecutor()).run(
        plan,
        plan["actions"][0]["action_id"],
        apply=True,
        approved_plan_id=plan["plan_id"],
        approved_snapshot_id=plan["source"]["current_snapshot_id"],
    )

    assert report.status == "reconciliation_failed"


def test_apply_rewrites_manifests_and_preserves_table_contents() -> None:
    plan = manifest_plan()
    action = next(
        action for action in plan["actions"] if action["action_type"] == "rewrite_manifests"
    )
    executor = FakeSqlExecutor()

    report = SparkMaintenanceExecutor(executor).run(
        plan,
        action["action_id"],
        apply=True,
        approved_plan_id=plan["plan_id"],
        approved_snapshot_id=plan["source"]["current_snapshot_id"],
    )

    assert report.status == "succeeded"
    assert report.before.manifest_count == 4
    assert report.after.manifest_count == 1
    assert report.before.data_file_count == report.after.data_file_count == 10
    assert report.before.record_count == report.after.record_count == 100
    procedure = next(sql for sql in executor.queries if "CALL" in sql)
    assert "rewrite_manifests" in procedure
    assert "use_caching => false" in procedure


def test_manifest_rewrite_rejects_state_above_safety_bound() -> None:
    plan = manifest_plan()
    action = next(
        action for action in plan["actions"] if action["action_type"] == "rewrite_manifests"
    )
    action["safety_bounds"]["max_manifests_to_rewrite"] = 3
    executor = FakeSqlExecutor()

    with pytest.raises(ExecutionContractError, match="manifest count exceeds"):
        SparkMaintenanceExecutor(executor).run(
            plan,
            action["action_id"],
            apply=True,
            approved_plan_id=plan["plan_id"],
            approved_snapshot_id=plan["source"]["current_snapshot_id"],
        )

    assert all("CALL" not in sql for sql in executor.queries)
