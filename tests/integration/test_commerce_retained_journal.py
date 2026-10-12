"""Opt-in read-only inspection of retained abrupt-owner-exit acceptance evidence."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
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
    report = inspect_journal(journal, limit=2, checkpoint=checkpoint)
    assert [r["outcome"] for r in report["runs"]] == ["ready", "unfinished"]
    successful, interrupted = report["runs"]
    assert interrupted["phase"] == "spark-commerce-daily-gold"
    assert interrupted["event_count"] == 17
    assert interrupted["checkpoint_completed_events"] == 0
    assert successful["checkpoint_completed_events"] == 1
    assert successful["run_id"] != interrupted["run_id"]
    assert successful["batch_id"] == interrupted["batch_id"]
    assert successful["manifest_sha256"] == interrupted["manifest_sha256"]
    assert successful["checkpoint_observation"] == "matching_manifest"
    assert interrupted["checkpoint_observation"] == "matching_manifest"
    after = [hashlib.sha256(path.read_bytes()).hexdigest() for path in (journal, checkpoint)]
    assert after == before


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_OWNER_OWNERSHIP_SMOKE") != "1",
    reason="requires retained owner container ownership evidence",
)
def test_retained_owner_and_retry_keep_distinct_container_ownership() -> None:
    directory = Path(os.environ["LAKEOPS_OWNER_EVIDENCE_DIRECTORY"])
    evidence = json.loads((directory / "owner-exit.json").read_text())
    assert evidence["status"] == "ready"
    journal = directory / "journal.sqlite3"
    before = hashlib.sha256(journal.read_bytes()).hexdigest()
    with sqlite3.connect(f"{journal.as_uri()}?mode=ro", uri=True) as db:
        rows = db.execute(
            "SELECT run_id,batch_id,manifest_sha256,service,container_name,project_hint,event "
            "FROM commerce_container_events ORDER BY sequence"
        ).fetchall()
    assert len(rows) == 32
    owner_id = evidence["incomplete_owner_events"][0][0]
    successful, interrupted = inspect_journal(journal, limit=2)["runs"]
    assert successful["outcome"] == "ready"
    assert interrupted["outcome"] == "unfinished"
    assert interrupted["run_id"] == owner_id
    retry_id = successful["run_id"]
    assert owner_id != retry_id
    assert {row[0] for row in rows} == {owner_id, retry_id}
    assert len({row[4] for row in rows}) == 16
    for run_id in (owner_id, retry_id):
        attempt = [row for row in rows if row[0] == run_id]
        assert len({row[3] for row in attempt}) == 8
        for container in {row[4] for row in attempt}:
            events = [row for row in attempt if row[4] == container]
            assert [row[6] for row in events] == ["launch_intent", "completed"]
            assert events[0][:6] == events[1][:6]
            assert events[0][1] == evidence["retry"]["batch_id"]
            assert events[0][2] == successful["manifest_sha256"]
            assert events[0][5].startswith("lakehouse-ops-owner-exit")
    assert hashlib.sha256(journal.read_bytes()).hexdigest() == before
