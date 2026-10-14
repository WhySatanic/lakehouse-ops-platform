"""Read-only validation of retained real reference-stack correction evidence."""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
from pathlib import Path

import pytest

from lakehouse_ops.commerce_history_correction import CORRECTED_HISTORY
from lakehouse_ops.commerce_journal_inspection import inspect_journal
from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES, CURRENT_STAGE
from lakehouse_ops.commerce_recovery import verify_recovery_audit
from lakehouse_ops.commerce_run_journal import CommerceRunJournal


@pytest.mark.skipif(os.environ.get("LAKEOPS_RUN_HISTORY_CORRECTION_SMOKE") != "1",
                    reason="requires retained real history-correction evidence")
def test_retained_correction_has_two_ready_maintenance_runs_and_no_checkpoint():
    root = Path(os.environ["LAKEOPS_HISTORY_CORRECTION_DIRECTORY"]).resolve()
    _check_retained_correction(root)


def _check_retained_correction(root):
    paths = [root / name for name in ("linked.json", "checkpoint.json", "run-journal.sqlite3")]
    before = [hashlib.sha256(path.read_bytes()).hexdigest() for path in paths]
    report = json.loads(paths[0].read_text(encoding="utf-8"))
    assert report["status"] == "ready"
    correction = report["late_customer_correction"]
    assert correction["status"] == "ready" and correction["history_snapshots_added"] == 1
    assert correction["replay"] == "snapshot_stable" and correction["checkpoint"] == "unchanged"
    assert correction["oracle"]["customer_history"] == CORRECTED_HISTORY
    assert correction["oracle"]["captured_revenue_cents"] == 2200
    checkpoint = json.loads(paths[1].read_text(encoding="utf-8"))
    completed_late = "late_batch_completion" in report
    following = report.get("next_batch_after_correction")
    recovery = report.get("late_batch_recovery")
    expected_completed = (4 if completed_late else 3) + int(following is not None)
    assert len(checkpoint["processed_batches"]) == expected_completed
    assert (correction["batch_id"] in checkpoint["processed_batches"]) == completed_late
    runs = inspect_journal(paths[2])["runs"]
    assert len(runs) == expected_completed + 3 + int(recovery is not None)
    failed = [run for run in runs if run["outcome"] != "ready"]
    if recovery is None:
        assert not failed
    else:
        assert completed_late and recovery["status"] == "ready"
        assert recovery["batch_id"] == correction["batch_id"]
        assert recovery["failure_boundary"] == "after_batch_gold_before_current"
        assert recovery["checkpoint_after_failure"] == "unchanged"
        assert recovery["batch_gold_replay"] == "rows_and_files_stable"
        assert recovery["checkpoint_before_sha256"] == recovery["checkpoint_after_failure_sha256"]
        assert len(recovery["checkpoint_before_sha256"]) == 64
        initial_snapshots = recovery["snapshots_before"]
        assert recovery["snapshots_after_failure"] == {
            **initial_snapshots, "commerce_daily": initial_snapshots["commerce_daily"] + 1,
        }
        previous = recovery["snapshots_after_failure"]
        replay = recovery["snapshots_before_retry_checkpoint"]
        assert replay["commerce_daily"] >= previous["commerce_daily"]
        for table in ("commerce_current_daily", "dim_customers_scd2"):
            assert replay[table] == previous[table]
        audit_before = recovery["published_after_failure"]
        audit_after = recovery["published_before_retry_checkpoint"]
        verify_recovery_audit(audit_before, audit_after)
        for audit, counts in ((audit_before, previous), (audit_after, replay)):
            for row in audit:
                assert row["rows"] >= 0 and row["files"] >= 0
                for field in ("rows_sha256", "files_sha256"):
                    assert len(row[field]) == 64 and int(row[field], 16) >= 0
            by_table = {row["table"]: row for row in audit}
            assert len(by_table) == len(audit)
            for table, count in counts.items():
                assert by_table[f"gold.{table}"]["snapshots"] == count
        assert recovery["oracle"]["captured_revenue_cents"] == 2200
        assert recovery["oracle"]["customer_history"] == CORRECTED_HISTORY
        assert recovery["batch_gold_after_failure"]["orders"] == 1
        assert recovery["batch_gold_after_failure"]["captured_revenue_cents"] == 1000
        assert len(failed) == 1 and failed[0]["outcome"] == "failed"
        assert failed[0]["run_id"] == recovery["failed_run_id"]
        assert failed[0]["phase"] == COMMERCE_STAGES[-1]
        assert failed[0]["error_type"] == "LateBatchDrillFailure"
        assert failed[0]["checkpoint_completed_events"] == 0
        retries = [run for run in runs if run["run_id"] == recovery["retry_run_id"]]
        assert len(retries) == 1 and retries[0]["outcome"] == "ready"
        assert retries[0]["checkpoint_completed_events"] == 1
        assert retries[0]["run_id"] != failed[0]["run_id"]
        for attempt in (failed[0], retries[0]):
            assert attempt["batch_id"] == correction["batch_id"]
            assert attempt["manifest_sha256"] == checkpoint["processed_batches"][
                correction["batch_id"]
            ]["manifest_sha256"]
        for field in ("failed_attempt_seconds", "retry_seconds"):
            value = recovery[field]
            assert type(value) in (int, float) and math.isfinite(value) and value > 0
    assert sum(run["checkpoint_completed_events"] for run in runs) == expected_completed
    if following is not None:
        assert completed_late and following["status"] == "ready"
        assert following["batch_id"] in checkpoint["processed_batches"]
        assert following["completed_stages"][-1] == "spark-commerce-current-gold"
        assert following["oracle"]["captured_revenue_cents"] == 2200
        assert following["oracle"]["customer_history"] == CORRECTED_HISTORY
        assert report["idle_after_next_batch"]["status"] == "idle"
    if completed_late:
        completion = report["late_batch_completion"]
        assert completion["status"] == "ready" and completion["batch_id"] == correction["batch_id"]
        assert report["idle_after_correction"]["status"] == "idle"
        assert completion["completed_stages"][-1] == "spark-commerce-current-gold"
    maintenance = [run for run in runs if run["run_id"] in correction["runs"]]
    assert len(maintenance) == 2 and len(set(correction["runs"])) == 2
    assert all(run["checkpoint_completed_events"] == 0 for run in maintenance)
    with sqlite3.connect(f"{paths[2].as_uri()}?mode=ro", uri=True) as connection:
        rows = connection.execute(
            "SELECT run_id, container_name, event, service FROM commerce_container_events "
            "ORDER BY sequence",
        ).fetchall()
    owned = [row for row in rows if row[0] in correction["runs"]]
    assert len(owned) == 10 and len({row[1] for row in owned}) == 5
    assert sum(row[2] == "launch_intent" for row in owned) == 5
    assert sum(row[2] == "completed" for row in owned) == 5
    if recovery is not None:
        for run_id, services in ((recovery["failed_run_id"], COMMERCE_STAGES),
                                 (recovery["retry_run_id"], (*COMMERCE_STAGES, CURRENT_STAGE))):
            attempt_owned = [row for row in rows if row[0] == run_id]
            assert len(attempt_owned) == len(services) * 2
            assert len({row[1] for row in attempt_owned}) == len(services)
            for event in ("launch_intent", "completed"):
                assert [row[3] for row in attempt_owned if row[2] == event] == list(services)
    assert [hashlib.sha256(path.read_bytes()).hexdigest() for path in paths] == before


@pytest.mark.parametrize("corruption", [None, "metadata_snapshot", "failed_checkpoint", "failed_id",
                                       "retry_id",
                                       "failed_phase", "replay", "checkpoint", "extra_failure",
                                       "snapshot", "checkpoint_hash", "gold_result", "row_hash",
                                       "file_hash", "missing_audit"])
def test_retained_recovery_requires_failed_attempt_without_checkpoint(tmp_path, corruption):
    """Synthetic journal validator tests, not proof of real Docker recovery."""
    batch = {"batch_id": "e" * 16, "manifest_sha256": "a" * 64}
    journal_path = tmp_path / "run-journal.sqlite3"
    oracle = {"customer_history": CORRECTED_HISTORY, "captured_revenue_cents": 2200}
    maintenance = []

    def record_run(selected, stages, *, checkpoint=False, failure=False):
        journal = CommerceRunJournal(journal_path)
        journal.record("planning", "selected", batch=selected)
        for service in stages:
            container = f"owned-{journal.run_id}-{service}"
            for event in ("launch_intent", "completed"):
                journal.record_container(service, container, event, batch=selected,
                                         project_hint="lakehouse-ops-linked-unit")
        if checkpoint:
            journal.record("checkpoint", "completed")
        journal.record(COMMERCE_STAGES[-1] if failure else "run",
                       "failed" if failure else "ready",
                       error_type="LateBatchDrillFailure" if failure else None)
        return journal.run_id

    completed_ids = [f"{number:016x}" for number in range(1, 4)]
    for batch_id in completed_ids:
        record_run({**batch, "batch_id": batch_id}, (), checkpoint=True)
    record_run({**batch, "batch_id": completed_ids[-1]}, ())
    for stages in (["bronze-input-sync", "spark-commerce-bronze",
                    "spark-commerce-customer-silver", "spark-commerce-customer-scd2"],
                   ["spark-commerce-customer-scd2"]):
        maintenance.append(record_run(batch, stages))
    failed_id = record_run(batch, COMMERCE_STAGES,
                           checkpoint=corruption == "failed_checkpoint", failure=True)
    retry_id = record_run(batch, (*COMMERCE_STAGES, CURRENT_STAGE), checkpoint=True)
    recovery = {"status": "ready", "batch_id": batch["batch_id"],
                "failed_run_id": failed_id, "retry_run_id": retry_id,
                "checkpoint_after_failure": "unchanged",
                "batch_gold_replay": "rows_and_files_stable",
                "failure_boundary": "after_batch_gold_before_current",
                "failed_attempt_seconds": 1.0, "retry_seconds": 2.0}
    before_snapshots = {"commerce_daily": 3, "commerce_current_daily": 3, "dim_customers_scd2": 4}
    after_snapshots = {**before_snapshots, "commerce_daily": 4}
    recovery.update(checkpoint_before_sha256="b" * 64, checkpoint_after_failure_sha256="b" * 64,
                    snapshots_before=before_snapshots, snapshots_after_failure=after_snapshots,
                    snapshots_before_retry_checkpoint=dict(after_snapshots), oracle=oracle,
                    batch_gold_after_failure={"orders": 1, "captured_revenue_cents": 1000})
    audit = [{"table": f"gold.{table}", "rows": 1, "files": 1,
              "rows_sha256": "a" * 64, "files_sha256": "b" * 64, "snapshots": count}
             for table, count in after_snapshots.items()]
    recovery.update(published_after_failure=audit,
                    published_before_retry_checkpoint=[dict(row) for row in audit])
    report = {"status": "ready", "late_customer_correction": {
        "status": "ready", "history_snapshots_added": 1, "replay": "snapshot_stable",
        "checkpoint": "unchanged", "oracle": oracle, "batch_id": batch["batch_id"],
        "runs": maintenance,
    }, "late_batch_completion": {"status": "ready", "batch_id": batch["batch_id"],
                                  "completed_stages": [*COMMERCE_STAGES, CURRENT_STAGE]},
        "idle_after_correction": {"status": "idle"}, "late_batch_recovery": recovery}
    if corruption == "failed_id":
        recovery["failed_run_id"] = "unknown"
    elif corruption == "retry_id":
        recovery["retry_run_id"] = failed_id
    elif corruption == "failed_phase":
        with sqlite3.connect(journal_path) as connection:
            connection.execute("UPDATE commerce_run_events SET phase='planning' "
                               "WHERE run_id=? AND event='failed'", (failed_id,))
    elif corruption == "replay":
        recovery["batch_gold_replay"] = "changed"
    elif corruption == "checkpoint":
        recovery["checkpoint_after_failure"] = "changed"
    elif corruption == "extra_failure":
        record_run(batch, (), failure=True)
    elif corruption == "snapshot":
        recovery["snapshots_before_retry_checkpoint"]["commerce_daily"] += 1
    elif corruption == "metadata_snapshot":
        recovery["snapshots_before_retry_checkpoint"]["commerce_daily"] += 1
        recovery["published_before_retry_checkpoint"][0]["snapshots"] += 1
    elif corruption == "row_hash":
        recovery["published_before_retry_checkpoint"][0]["rows_sha256"] = "c" * 64
    elif corruption == "file_hash":
        recovery["published_before_retry_checkpoint"][0]["files_sha256"] = "c" * 64
    elif corruption == "missing_audit":
        recovery["published_before_retry_checkpoint"] = []
    elif corruption == "checkpoint_hash":
        recovery["checkpoint_after_failure_sha256"] = "c" * 64
    elif corruption == "gold_result":
        recovery["batch_gold_after_failure"]["captured_revenue_cents"] = 1100
    (tmp_path / "linked.json").write_text(json.dumps(report))
    (tmp_path / "checkpoint.json").write_text(json.dumps({"processed_batches": {
        batch_id: {"manifest_sha256": batch["manifest_sha256"]}
        for batch_id in [*completed_ids, batch["batch_id"]]
    }}))
    if corruption not in (None, "metadata_snapshot"):
        with pytest.raises((AssertionError, ValueError)):
            _check_retained_correction(tmp_path)
    else:
        _check_retained_correction(tmp_path)
