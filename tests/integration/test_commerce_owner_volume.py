"""Opt-in read-only verification of a retained forced-exit volume/memory observation."""

import hashlib
import json
import os
from pathlib import Path

import pytest

from lakehouse_ops.commerce_journal_inspection import inspect_journal


@pytest.mark.skipif(os.getenv("LAKEOPS_RUN_OWNER_VOLUME_SMOKE") != "1",
                    reason="requires retained measured owner-exit reference-stack evidence")
def test_retained_owner_volume_timing_memory_and_verified_retry():
    directory = Path(os.environ["LAKEOPS_OWNER_EVIDENCE_DIRECTORY"])
    journal, checkpoint = directory / "journal.sqlite3", directory / "checkpoint.json"
    before = [hashlib.sha256(path.read_bytes()).hexdigest() for path in (journal, checkpoint)]
    report = json.loads((directory / "owner-exit.json").read_text(encoding="utf-8"))
    assert report["status"] == "ready" and report["owner_exit_code"] != 0
    assert report["checkpoint_before_retry"] == "absent"
    assert report["idle"]["status"] == "idle"
    assert 100 <= report["fixture_orders"] <= 10000
    assert report["retry"]["verification"]["orders"] == report["fixture_orders"]
    assert set(report["phase_elapsed_seconds"]) == {
        "interrupted_attempt", "audit_before_retry", "retry", "audit_after_retry", "idle_replay",
    }
    assert all(value >= 0 for value in report["phase_elapsed_seconds"].values())
    successful, interrupted = inspect_journal(journal, limit=2)["runs"]
    assert [successful["outcome"], interrupted["outcome"]] == ["ready", "unfinished"]
    assert successful["run_id"] != interrupted["run_id"]
    assert interrupted["checkpoint_completed_events"] == 0
    assert successful["checkpoint_completed_events"] == 1
    for name in ("owner.log", "retry.log"):
        memory = [json.loads(line) for line in
                  (directory / name).read_text(encoding="utf-8").splitlines()
                  if '"kind": "spark_container_memory"' in line]
        assert len(memory) == 7
        assert all(item["status"] == "observed" and item["peak_bytes"] > 0 for item in memory)
        assert all(item["batch_id"] == report["retry"]["batch_id"] for item in memory)
    after = [hashlib.sha256(path.read_bytes()).hexdigest() for path in (journal, checkpoint)]
    assert after == before
