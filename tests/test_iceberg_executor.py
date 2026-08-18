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
        self.snapshot_ids = [
            snapshot_id,
            "8740000000000000001",
            "8730000000000000001",
            "8720000000000000001",
            "8710000000000000001",
        ]
        self.file_count = 10
        self.record_count = 100
        self.manifest_count = 4
        self.orphan_files = [
            "s3a://lakehouse/warehouse/silver.db/weather_hourly/data/zombie-b.parquet",
            "s3a://lakehouse/warehouse/silver.db/weather_hourly/data/zombie-a.parquet",
        ]
        self.queries: list[str] = []

    def query(self, sql: str) -> list[dict[str, Any]]:
        self.queries.append(sql)
        if ".history" in sql:
            return [{"snapshot_id": self.snapshot_id}]
        if ".snapshots" in sql:
            return [{"snapshot_id": snapshot_id} for snapshot_id in self.snapshot_ids]
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
            self.snapshot_ids.insert(0, self.snapshot_id)
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
            self.snapshot_ids.insert(0, self.snapshot_id)
            self.manifest_count = 1
            return [
                {
                    "rewritten_manifests_count": 4,
                    "added_manifests_count": 1,
                }
            ]
        if "expire_snapshots" in sql:
            self.snapshot_ids = self.snapshot_ids[:2]
            return [
                {
                    "deleted_data_files_count": 0,
                    "deleted_position_delete_files_count": 0,
                    "deleted_equality_delete_files_count": 0,
                    "deleted_manifest_files_count": 3,
                    "deleted_manifest_lists_count": 3,
                    "deleted_statistics_files_count": 0,
                }
            ]
        if "remove_orphan_files" in sql:
            result = [
                {"orphan_file_location": location} for location in self.orphan_files
            ]
            if "dry_run => false" in sql:
                self.orphan_files = []
            return result
        if sql.startswith("CREATE OR REPLACE TEMP VIEW"):
            return []
        if sql.startswith("DROP VIEW IF EXISTS"):
            return []
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


def expiration_plan() -> dict[str, Any]:
    snapshot_ids = [
        "8710000000000000001",
        "8720000000000000001",
        "8730000000000000001",
        "8740000000000000001",
        "8750000000000000001",
    ]
    report = {
        "schema_version": "1.0",
        "status": "ready",
        "collected_at": "2026-08-18T13:30:00+00:00",
        "table": "lakehouse.silver.weather_hourly",
        "snapshots": {
            "current_id": snapshot_ids[-1],
            "history": [
                {
                    "snapshot_id": snapshot_id,
                    "committed_at": f"2026-08-{day:02d}T13:00:00+00:00",
                }
                for snapshot_id, day in zip(
                    snapshot_ids, (10, 11, 12, 17, 18), strict=True
                )
            ],
        },
        "references": [
            {"name": "main", "reference_type": "BRANCH", "snapshot_id": snapshot_ids[-1]}
        ],
        "files": {
            "count": 10,
            "total_size_bytes": 10 * 128 * 1024 * 1024,
            "delete_file_count": 0,
        },
        "manifests": {"count": 4},
    }
    policy = MaintenancePolicy(snapshot_retention_hours=24, min_snapshots_to_keep=2)
    return IcebergMaintenancePlanner(policy).plan(report).as_dict()


def orphan_inspection_plan(*, max_orphan_files: int = 1000) -> dict[str, Any]:
    report = {
        "schema_version": "1.0",
        "status": "ready",
        "collected_at": "2026-08-18T13:30:00+00:00",
        "table": "lakehouse.silver.weather_hourly",
        "snapshots": {
            "current_id": "8750000000000000001",
            "history": [
                {
                    "snapshot_id": "8750000000000000001",
                    "committed_at": "2026-08-18T13:00:00+00:00",
                }
            ],
        },
        "references": [
            {
                "name": "main",
                "reference_type": "BRANCH",
                "snapshot_id": "8750000000000000001",
            }
        ],
        "files": {
            "count": 10,
            "total_size_bytes": 10 * 128 * 1024 * 1024,
            "delete_file_count": 0,
        },
        "manifests": {"count": 4},
    }
    policy = MaintenancePolicy(
        orphan_inspection_enabled=True,
        orphan_retention_hours=168,
        max_orphan_files=max_orphan_files,
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


def test_apply_expires_exact_snapshot_batch_and_preserves_current_state() -> None:
    plan = expiration_plan()
    action = next(
        action for action in plan["actions"] if action["action_type"] == "expire_snapshots"
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
    assert report.before.snapshot_count == 5
    assert report.after.snapshot_count == 2
    assert report.before.snapshot_id == report.after.snapshot_id
    assert report.before.record_count == report.after.record_count == 100
    procedure = next(sql for sql in executor.queries if "CALL" in sql)
    assert (
        "snapshot_ids => ARRAY(8710000000000000001, 8720000000000000001, "
        "8730000000000000001)" in procedure
    )
    assert "CAST(" not in procedure
    assert "max_concurrent_deletes => 1" in procedure
    assert "stream_results => true" in procedure
    assert "clean_expired_metadata => false" in procedure


def test_expiration_rejects_changed_snapshot_history_before_procedure() -> None:
    plan = expiration_plan()
    action = next(
        action for action in plan["actions"] if action["action_type"] == "expire_snapshots"
    )
    executor = FakeSqlExecutor()
    executor.snapshot_ids.insert(1, "8745000000000000001")

    with pytest.raises(ExecutionContractError, match="snapshot history"):
        SparkMaintenanceExecutor(executor).run(
            plan,
            action["action_id"],
            apply=True,
            approved_plan_id=plan["plan_id"],
            approved_snapshot_id=plan["source"]["current_snapshot_id"],
        )

    assert all("CALL" not in sql for sql in executor.queries)


@pytest.mark.parametrize(
    ("target_id", "message"),
    [
        ("8750000000000000001", "current snapshot cannot be expired"),
        ("9990000000000000001", "expiration target is absent"),
    ],
)
def test_expiration_rejects_unsafe_target_ids(
    target_id: str, message: str
) -> None:
    plan = expiration_plan()
    action = next(
        action for action in plan["actions"] if action["action_type"] == "expire_snapshots"
    )
    action["parameters"]["snapshot_ids"] = [target_id]
    executor = FakeSqlExecutor()

    with pytest.raises(ExecutionContractError, match=message):
        SparkMaintenanceExecutor(executor).run(
            plan,
            action["action_id"],
            apply=True,
            approved_plan_id=plan["plan_id"],
            approved_snapshot_id=plan["source"]["current_snapshot_id"],
        )

    assert all("CALL" not in sql for sql in executor.queries)


def test_orphan_inspection_returns_sorted_review_evidence() -> None:
    plan = orphan_inspection_plan()
    action = next(
        action
        for action in plan["actions"]
        if action["action_type"] == "inspect_orphan_files"
    )
    executor = FakeSqlExecutor()

    report = SparkMaintenanceExecutor(executor).run(plan, action["action_id"])

    assert report.status == "inspection_complete"
    assert report.applied is False
    assert report.before == report.after
    assert report.procedure_result == {"orphan_file_count": 2}
    assert report.candidate_set_id == "orphans-bb716dc042e82997"
    assert report.candidate_files == tuple(sorted(executor.orphan_files))
    procedure = next(sql for sql in executor.queries if "CALL" in sql)
    assert "remove_orphan_files" in procedure
    assert "older_than => TIMESTAMP '2026-08-11 13:30:00.000000'" in procedure
    assert "dry_run => true" in procedure
    assert "stream_results => false" in procedure
    assert "prefix_listing => true" in procedure
    assert "prefix_mismatch_mode => 'ERROR'" in procedure


def test_orphan_removal_requires_exact_candidate_approval() -> None:
    plan = orphan_inspection_plan()
    action = next(
        action
        for action in plan["actions"]
        if action["action_type"] == "inspect_orphan_files"
    )
    executor = FakeSqlExecutor()
    inspection = SparkMaintenanceExecutor(executor).run(
        plan, action["action_id"]
    ).as_dict()

    with pytest.raises(ExecutionContractError, match="candidate set ID"):
        SparkMaintenanceExecutor(executor).run(
            plan,
            action["action_id"],
            apply=True,
            approved_plan_id=plan["plan_id"],
            approved_snapshot_id=plan["source"]["current_snapshot_id"],
            approved_candidate_set_id="orphans-wrong",
            candidate_report=inspection,
        )


def test_orphan_removal_deletes_only_the_approved_candidate_set() -> None:
    plan = orphan_inspection_plan()
    action = next(
        action
        for action in plan["actions"]
        if action["action_type"] == "inspect_orphan_files"
    )
    executor = FakeSqlExecutor()
    inspection = SparkMaintenanceExecutor(executor).run(
        plan, action["action_id"]
    ).as_dict()

    report = SparkMaintenanceExecutor(executor).run(
        plan,
        action["action_id"],
        apply=True,
        approved_plan_id=plan["plan_id"],
        approved_snapshot_id=plan["source"]["current_snapshot_id"],
        approved_candidate_set_id=inspection["candidate_set_id"],
        candidate_report=inspection,
    )

    assert report.status == "succeeded"
    assert report.applied is True
    assert report.before == report.after
    assert report.candidate_files == tuple(sorted(inspection["candidate_files"]))
    assert report.procedure_result == {
        "orphan_file_count": 2,
        "deleted_orphan_file_count": 2,
    }
    assert executor.orphan_files == []
    calls = [sql for sql in executor.queries if "remove_orphan_files" in sql]
    assert "file_list_view => 'lakehouse_ops_orphans_bb716dc042e82997'" in calls[-1]
    assert "dry_run => true" in calls[-2]
    assert "dry_run => false" in calls[-1]
    assert "max_concurrent_deletes => 1" in calls[-1]


def test_orphan_removal_rejects_changed_table_or_candidate_state() -> None:
    plan = orphan_inspection_plan()
    action = next(
        action
        for action in plan["actions"]
        if action["action_type"] == "inspect_orphan_files"
    )
    executor = FakeSqlExecutor()
    inspection = SparkMaintenanceExecutor(executor).run(
        plan, action["action_id"]
    ).as_dict()
    executor.record_count += 1

    with pytest.raises(ExecutionContractError, match="table state changed"):
        SparkMaintenanceExecutor(executor).run(
            plan,
            action["action_id"],
            apply=True,
            approved_plan_id=plan["plan_id"],
            approved_snapshot_id=plan["source"]["current_snapshot_id"],
            approved_candidate_set_id=inspection["candidate_set_id"],
            candidate_report=inspection,
        )

    executor.record_count -= 1
    executor.orphan_files.pop()
    with pytest.raises(ExecutionContractError, match="no longer entirely orphaned"):
        SparkMaintenanceExecutor(executor).run(
            plan,
            action["action_id"],
            apply=True,
            approved_plan_id=plan["plan_id"],
            approved_snapshot_id=plan["source"]["current_snapshot_id"],
            approved_candidate_set_id=inspection["candidate_set_id"],
            candidate_report=inspection,
        )


def test_orphan_removal_is_noop_for_an_approved_empty_set() -> None:
    plan = orphan_inspection_plan()
    action = next(
        action
        for action in plan["actions"]
        if action["action_type"] == "inspect_orphan_files"
    )
    executor = FakeSqlExecutor()
    executor.orphan_files = []
    inspection = SparkMaintenanceExecutor(executor).run(
        plan, action["action_id"]
    ).as_dict()

    report = SparkMaintenanceExecutor(executor).run(
        plan,
        action["action_id"],
        apply=True,
        approved_plan_id=plan["plan_id"],
        approved_snapshot_id=plan["source"]["current_snapshot_id"],
        approved_candidate_set_id=inspection["candidate_set_id"],
        candidate_report=inspection,
    )

    assert report.status == "noop"
    assert report.applied is False
    assert report.before == report.after
    assert report.procedure_result["deleted_orphan_file_count"] == 0
    assert all("dry_run => false" not in sql for sql in executor.queries)


def test_orphan_inspection_rejects_oversized_inventory() -> None:
    plan = orphan_inspection_plan(max_orphan_files=1)
    action = next(
        action
        for action in plan["actions"]
        if action["action_type"] == "inspect_orphan_files"
    )

    with pytest.raises(ExecutionContractError, match="exceeds the review"):
        SparkMaintenanceExecutor(FakeSqlExecutor()).run(plan, action["action_id"])
