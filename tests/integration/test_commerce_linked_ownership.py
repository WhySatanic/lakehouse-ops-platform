"""Opt-in read-only verification of a completed reference-stack linked scenario."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections import Counter
from pathlib import Path

import pytest

from lakehouse_ops.commerce_journal_inspection import inspect_journal


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_LINKED_OWNERSHIP_SMOKE") != "1",
    reason="requires fresh linked reference-stack evidence",
)
def test_linked_containers_share_attempt_identity_without_replay_checkpoint():
    directory = Path(os.environ["LAKEOPS_LINKED_OWNERSHIP_DIRECTORY"])
    journal = directory / "run-journal.sqlite3"
    paths = [journal, directory / "checkpoint.json", directory / "linked.json"]
    before = [hashlib.sha256(path.read_bytes()).hexdigest() for path in paths]
    report = json.loads(paths[-1].read_text())
    assert report["status"] == "ready"
    assert report["current_mart_replay"] == "snapshot_stable"
    assert report["idle"]["status"] == "idle"
    attempts = inspect_journal(journal)["runs"]
    assert len(attempts) == 4
    assert all(attempt["outcome"] == "ready" for attempt in attempts)
    assert [attempt["checkpoint_completed_events"] for attempt in attempts] == [0, 1, 1, 1]
    assert attempts[0]["batch_id"] == attempts[1]["batch_id"]
    by_id = {attempt["run_id"]: attempt for attempt in attempts}
    with sqlite3.connect(journal.resolve().as_uri() + "?mode=ro", uri=True) as connection:
        rows = connection.execute(
            "SELECT run_id,batch_id,manifest_sha256,container_name,project_hint,event "
            "FROM commerce_container_events ORDER BY sequence",
        ).fetchall()
    assert len(rows) == 56
    assert len({row[3] for row in rows}) == 28
    assert Counter(row[0] for row in rows) == {
        attempt["run_id"]: 2 if i == 0 else 18 for i, attempt in enumerate(attempts)
    }
    for intent, completed in zip(rows[::2], rows[1::2], strict=True):
        assert intent[:-1] == completed[:-1]
        assert (intent[-1], completed[-1]) == ("launch_intent", "completed")
        attempt = by_id[intent[0]]
        assert intent[1:3] == (attempt["batch_id"], attempt["manifest_sha256"])
        assert intent[4].startswith("lakehouse-ops-linked")
    assert [hashlib.sha256(path.read_bytes()).hexdigest() for path in paths] == before
