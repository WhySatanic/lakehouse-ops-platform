"""Read-only validation of retained real reference-stack correction evidence."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path

import pytest

from lakehouse_ops.commerce_history_correction import CORRECTED_HISTORY
from lakehouse_ops.commerce_journal_inspection import inspect_journal


@pytest.mark.skipif(os.environ.get("LAKEOPS_RUN_HISTORY_CORRECTION_SMOKE") != "1",
                    reason="requires retained real history-correction evidence")
def test_retained_correction_has_two_ready_maintenance_runs_and_no_checkpoint():
    root = Path(os.environ["LAKEOPS_HISTORY_CORRECTION_DIRECTORY"]).resolve()
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
    assert len(checkpoint["processed_batches"]) == (4 if completed_late else 3)
    assert (correction["batch_id"] in checkpoint["processed_batches"]) == completed_late
    runs = inspect_journal(paths[2])["runs"]
    assert len(runs) == (7 if completed_late else 6)
    assert all(run["outcome"] == "ready" for run in runs)
    assert sum(run["checkpoint_completed_events"] for run in runs) == (4 if completed_late else 3)
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
            "SELECT run_id, container_name, event FROM commerce_container_events",
        ).fetchall()
    owned = [row for row in rows if row[0] in correction["runs"]]
    assert len(owned) == 10 and len({row[1] for row in owned}) == 5
    assert sum(row[2] == "launch_intent" for row in owned) == 5
    assert sum(row[2] == "completed" for row in owned) == 5
    assert [hashlib.sha256(path.read_bytes()).hexdigest() for path in paths] == before
