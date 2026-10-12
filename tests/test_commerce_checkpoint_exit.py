"""Real owner exit at a local checkpoint boundary, not a Spark transaction drill."""

import json
import subprocess
import sys

import pytest

from lakehouse_ops.commerce_journal_inspection import inspect_journal, main
from lakehouse_ops.commerce_run_journal import CommerceRunJournal
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock
from lakehouse_ops.ingestion.commerce_batches import CommerceBatch, CommerceBatchPlanner

OWNER = """
import os, sys
from pathlib import Path
from lakehouse_ops.commerce_run_journal import CommerceRunJournal
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock
from lakehouse_ops.ingestion.commerce_batches import CommerceBatch, CommerceBatchPlanner
journal_path, state_path = map(Path, sys.argv[1:])
batch = CommerceBatch('a'*16, '2026-02-01T00:00:00+00:00', 'b'*64, 'fixture')
planner = CommerceBatchPlanner(None, bucket='test', state_path=state_path)
planner.discover = lambda: [batch]
with commerce_runner_lock(state_path.parent / 'owner.lock'):
    journal = CommerceRunJournal(journal_path)
    journal.record('checkpoint', 'started', batch=batch.as_dict())
    planner.commit(batch.batch_id, expected_manifest_sha256=batch.manifest_sha256)
    os._exit(23)
"""


def test_process_exit_after_checkpoint_does_not_require_replay(tmp_path):
    journal, state = tmp_path / "journal.sqlite3", tmp_path / "checkpoint.json"
    process = subprocess.run([sys.executable, "-c", OWNER, str(journal), str(state)], timeout=30)
    assert process.returncode == 23
    before = (journal.read_bytes(), state.read_bytes())
    run = inspect_journal(journal, checkpoint=state)["runs"][0]
    assert run["outcome"] == "unfinished"
    assert run["phase"] == "checkpoint" and run["event"] == "started"
    assert run["checkpoint_completed_events"] == 0
    assert run["checkpoint_observation"] == "matching_manifest"
    planner = CommerceBatchPlanner(None, bucket="test", state_path=state)
    planner.discover = lambda: [CommerceBatch("a" * 16, "2026-02-01T00:00:00+00:00",
                                              "b" * 64, "fixture")]
    with commerce_runner_lock(tmp_path / "owner.lock"):
        assert planner.plan(max_batches=1)["selected_batches"] == 0
        assert planner.commit("a" * 16)["created"] is False
    assert (journal.read_bytes(), state.read_bytes()) == before


@pytest.mark.parametrize("kind", ["matching", "different", "missing", "unbound"])
def test_checkpoint_observation_never_changes_journal_outcome(tmp_path, capsys, kind):
    journal, state = tmp_path / "journal.sqlite3", tmp_path / "checkpoint.json"
    batch = {"batch_id": "a" * 16, "manifest_sha256": "b" * 64}
    CommerceRunJournal(journal).record("checkpoint", "started",
                                     batch=None if kind == "unbound" else batch)
    if kind != "missing":
        state.write_text(json.dumps({"schema_version": 1, "processed_batches": {
            "a" * 16: {"batch_at": "2026-02-01T00:00:00+00:00",
                        "manifest_sha256": "c" * 64 if kind == "different" else "b" * 64},
        }}))
    before = journal.read_bytes()
    assert main(["--journal", str(journal), "--checkpoint", str(state)]) == 0
    run = json.loads(capsys.readouterr().out)["runs"][0]
    assert run["outcome"] == "unfinished"
    assert run["checkpoint_observation"] == {
        "matching": "matching_manifest", "different": "different_manifest",
        "missing": "not_recorded", "unbound": "not_batch_bound",
    }[kind]
    assert journal.read_bytes() == before
    assert state.exists() == (kind != "missing")
