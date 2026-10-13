from __future__ import annotations

import json

import pytest

from lakehouse_ops.commerce_change_scenario import generate_change_scenario
from lakehouse_ops.commerce_history_correction import CORRECTED_HISTORY, exercise_following_batch
from lakehouse_ops.commerce_journal_inspection import inspect_journal
from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES, CURRENT_STAGE


@pytest.mark.parametrize("failure", [None, "stage", "revenue", "history", "snapshot", "mode"])
def test_following_batch_oracle_precedes_checkpoint_and_idle(tmp_path, failure):
    generate_change_scenario(tmp_path / "scenario")
    expected = json.loads((tmp_path / "scenario/expected.json").read_text())
    batch = {"batch_id": "f" * 16, "manifest_sha256": "a" * 64}
    pending = True
    stages, commits = [], []
    snapshots_reads = 0

    class Planner:
        def plan(self, **kwargs):
            return {"batches": [batch] if pending else []}

        def execution_options(self, selected):
            return {"customer_history_mode": "rebuild" if failure == "mode" else "forward"}

        def commit(self, batch_id, **kwargs):
            nonlocal pending
            assert stages == [*COMMERCE_STAGES, CURRENT_STAGE]
            assert snapshots_reads == 2
            pending = False
            commits.append(batch_id)
            return {"batch_id": batch_id, "created": True}

    def stage(service, selected, *, journal):
        assert not commits and selected["batch_id"] == batch["batch_id"]
        stages.append(service)
        if failure == "stage" and service == CURRENT_STAGE:
            raise RuntimeError("current publication failed")

    def query(sql):
        nonlocal snapshots_reads
        if "$snapshots" in sql:
            snapshots_reads += 1
            return [{"snapshots": 4 + int(failure == "snapshot" and snapshots_reads == 2)}]
        if "commerce_current_daily" in sql:
            return [{"order_day": day, "captured_revenue_cents": (
                0 if failure == "revenue" else value
            ), "order_count": 1, "source_batch_id": batch["batch_id"]}
                for day, value in expected["latest_daily_revenue_cents"].items()]
        if "dim_customers_scd2" in sql:
            return [{**row, "customer_id": "C00000001", "is_current": row["valid_to"] is None,
                     "full_name": "wrong" if failure == "history" else row["full_name"]}
                    for row in CORRECTED_HISTORY]
        if "SELECT order_id" in sql:
            return expected["latest_orders"]
        return [{"days": 2, "orders": 2, "captured_revenue_cents": 2200, "invalid_days": 0}]

    report = {}
    if failure:
        with pytest.raises((ValueError, RuntimeError)):
            exercise_following_batch(Planner(), query, stage, batch, expected, tmp_path, report)
        assert commits == [] and pending
        assert "next_batch_after_correction" not in report
        run = inspect_journal(tmp_path / "run-journal.sqlite3")["runs"][0]
        assert run["outcome"] == "failed" and run["checkpoint_completed_events"] == 0
    else:
        exercise_following_batch(Planner(), query, stage, batch, expected, tmp_path, report)
        assert commits == [batch["batch_id"]] and not pending
        assert report["idle_after_next_batch"]["status"] == "idle"
        assert report["next_batch_after_correction"]["oracle"][
            "customer_history"
        ] == CORRECTED_HISTORY
