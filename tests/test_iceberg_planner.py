from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from lakehouse_ops.iceberg.planner import (
    IcebergMaintenancePlanner,
    MaintenancePolicy,
    PlanningContractError,
)


def metadata_report() -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "status": "ready",
        "collected_at": "2026-08-18T13:30:00+00:00",
        "table": "lakehouse.silver.weather_hourly",
        "snapshots": {
            "current_id": "8750000000000000001",
            "history": [
                {
                    "snapshot_id": "8730000000000000001",
                    "committed_at": "2026-08-16T13:00:00+00:00",
                },
                {
                    "snapshot_id": "8740000000000000001",
                    "committed_at": "2026-08-17T13:00:00+00:00",
                },
                {
                    "snapshot_id": "8750000000000000001",
                    "committed_at": "2026-08-18T13:00:00+00:00",
                },
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
            "count": 4,
            "total_size_bytes": 4 * 128 * 1024 * 1024,
            "delete_file_count": 0,
        },
        "manifests": {"count": 2},
    }


def test_healthy_table_has_no_actions() -> None:
    plan = IcebergMaintenancePlanner().plan(metadata_report()).as_dict()

    assert plan["schema_version"] == "1.0"
    assert plan["status"] == "healthy"
    assert plan["table"] == "lakehouse.silver.weather_hourly"
    assert plan["actions"] == ()
    assert [check["outcome"] for check in plan["checks"]] == [
        "healthy",
        "healthy",
        "healthy",
    ]
    assert plan["source"]["current_snapshot_id"] == "8750000000000000001"


def test_small_files_recommend_data_file_rewrite() -> None:
    report = metadata_report()
    report["files"]["count"] = 10
    report["files"]["total_size_bytes"] = 100 * 1024 * 1024

    first = IcebergMaintenancePlanner().plan(report).as_dict()
    second = IcebergMaintenancePlanner().plan(deepcopy(report)).as_dict()

    assert first == second
    assert first["status"] == "maintenance_recommended"
    action = first["actions"][0]
    assert action["action_type"] == "rewrite_data_files"
    assert action["reason_code"] == "small_files"
    assert action["safety_bounds"] == {
        "dry_run_required": True,
        "expected_snapshot_id": "8750000000000000001",
        "max_concurrent_jobs": 1,
        "max_files_to_rewrite": 1000,
    }
    assert action["action_id"].startswith("action-")


def test_dense_manifests_recommend_manifest_rewrite() -> None:
    report = metadata_report()
    report["manifests"]["count"] = 8

    plan = IcebergMaintenancePlanner().plan(report).as_dict()

    assert [action["action_type"] for action in plan["actions"]] == [
        "rewrite_manifests"
    ]
    assert plan["actions"][0]["safety_bounds"] == {
        "dry_run_required": True,
        "expected_snapshot_id": "8750000000000000001",
        "max_concurrent_jobs": 1,
        "max_manifests_to_rewrite": 1000,
    }
    assert plan["checks"][1]["outcome"] == "recommend"


def test_delete_files_defer_size_based_compaction() -> None:
    report = metadata_report()
    report["files"].update(count=5, delete_file_count=1, total_size_bytes=1024)

    plan = IcebergMaintenancePlanner().plan(report).as_dict()

    assert plan["status"] == "review_required"
    assert plan["checks"][0]["outcome"] == "deferred"
    assert plan["actions"] == ()


def test_old_snapshots_recommend_exact_expiration_batch() -> None:
    report = metadata_report()
    report["snapshots"]["history"] = [
        {
            "snapshot_id": str(snapshot_id),
            "committed_at": f"2026-08-{day:02d}T13:00:00+00:00",
        }
        for snapshot_id, day in zip(range(101, 106), (10, 11, 12, 17, 18), strict=True)
    ]
    report["snapshots"]["current_id"] = "105"
    report["references"][0]["snapshot_id"] = "105"
    policy = MaintenancePolicy(snapshot_retention_hours=24, min_snapshots_to_keep=2)

    plan = IcebergMaintenancePlanner(policy).plan(report).as_dict()
    action = next(
        action for action in plan["actions"] if action["action_type"] == "expire_snapshots"
    )

    assert action["parameters"]["snapshot_ids"] == ["101", "102", "103"]
    assert action["safety_bounds"]["max_snapshots_to_expire"] == 50
    assert action["safety_bounds"]["expected_history_snapshot_ids"] == [
        "101",
        "102",
        "103",
        "104",
        "105",
    ]


def test_non_main_reference_defers_snapshot_expiration() -> None:
    report = metadata_report()
    report["references"].append(
        {"name": "audit", "reference_type": "TAG", "snapshot_id": "8730000000000000001"}
    )

    plan = IcebergMaintenancePlanner().plan(report).as_dict()

    assert plan["checks"][2]["outcome"] == "deferred"
    assert all(action["action_type"] != "expire_snapshots" for action in plan["actions"])


def test_opt_in_orphan_inspection_uses_bounded_age_window() -> None:
    policy = MaintenancePolicy(
        orphan_inspection_enabled=True,
        orphan_retention_hours=96,
        max_orphan_files=25,
    )

    plan = IcebergMaintenancePlanner(policy).plan(metadata_report()).as_dict()
    action = next(
        action
        for action in plan["actions"]
        if action["action_type"] == "inspect_orphan_files"
    )

    assert action["parameters"] == {"older_than": "2026-08-14T13:30:00+00:00"}
    assert action["safety_bounds"]["max_orphan_files"] == 25
    assert plan["checks"][-1]["rule"] == "orphan_file_inventory"
    assert plan["checks"][-1]["outcome"] == "inspect"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema_version", "2.0", "unsupported metadata schema_version"),
        ("status", "failed", "metadata report status must be ready"),
        ("files", [], "files must be an object"),
    ],
)
def test_invalid_metadata_contract_is_rejected(
    field: str, value: object, message: str
) -> None:
    report = metadata_report()
    report[field] = value

    with pytest.raises(PlanningContractError, match=message):
        IcebergMaintenancePlanner().plan(report)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"target_file_size_bytes": 0},
        {"small_file_ratio": 0},
        {"small_file_ratio": 1.1},
        {"min_data_files": 0},
        {"min_manifest_count": 0},
        {"max_manifests_per_data_file": 0},
        {"snapshot_retention_hours": -1},
        {"min_snapshots_to_keep": 1},
        {"max_snapshots_to_expire": 0},
        {"orphan_retention_hours": 71},
        {"max_orphan_files": 0},
    ],
)
def test_invalid_policy_is_rejected(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        MaintenancePolicy(**kwargs)
