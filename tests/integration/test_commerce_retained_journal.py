"""Opt-in read-only inspection of retained abrupt-owner-exit acceptance evidence."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from lakehouse_ops.commerce_journal_inspection import inspect_journal


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_JOURNAL_INSPECTION_SMOKE") != "1",
    reason="requires retained owner evidence",
)
def test_retained_interrupted_owner_and_retry_are_read_only() -> None:
    directory = Path(os.environ["LAKEOPS_OWNER_EVIDENCE_DIRECTORY"])
    journal = directory / "journal.sqlite3"
    checkpoint = directory / "checkpoint.json"
    before = [hashlib.sha256(path.read_bytes()).hexdigest() for path in (journal, checkpoint)]
    report = inspect_journal(journal, limit=2)
    assert [r["outcome"] for r in report["runs"]] == ["ready", "unfinished"]
    successful, interrupted = report["runs"]
    assert interrupted["phase"] == "spark-commerce-daily-gold"
    assert interrupted["event_count"] == 17
    assert interrupted["checkpoint_completed_events"] == 0
    assert successful["checkpoint_completed_events"] == 1
    assert successful["run_id"] != interrupted["run_id"]
    assert successful["batch_id"] == interrupted["batch_id"]
    assert successful["manifest_sha256"] == interrupted["manifest_sha256"]
    after = [hashlib.sha256(path.read_bytes()).hexdigest() for path in (journal, checkpoint)]
    assert after == before
