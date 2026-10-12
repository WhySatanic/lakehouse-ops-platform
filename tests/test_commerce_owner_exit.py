from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from lakehouse_ops import commerce_owner_exit as drill
from lakehouse_ops.commerce_run_journal import CommerceRunJournal
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock


def child(connection, path, mode):
    with commerce_runner_lock(path / "lock"):
        journal = CommerceRunJournal(path / "journal.sqlite3")
        journal.record(drill.COMMERCE_STAGES[-1], "started")
        if mode == "early":
            return
        if mode != "timeout":
            connection.send({"boundary": "wrong" if mode == "wrong" else "gold_published",
                             "pid": os.getpid()})
        time.sleep(30)


@pytest.mark.parametrize("mode", ["ready", "wrong", "early", "timeout"])
def test_actual_owned_process_kill_and_lock_release(tmp_path: Path, mode):
    if mode == "ready":
        assert drill.force_owner_exit(child, (tmp_path, mode), timeout=5) != 0
        with sqlite3.connect(tmp_path / "journal.sqlite3") as db:
            assert db.execute("SELECT event FROM commerce_run_events").fetchall() == [("started",)]
    else:
        with pytest.raises((ValueError, RuntimeError, TimeoutError)):
            drill.force_owner_exit(child, (tmp_path, mode), timeout=2)
    with commerce_runner_lock(tmp_path / "lock"):
        pass


@pytest.mark.parametrize("failure", [None, "existing", "checkpoint", "journal", "files", "idle"])
def test_main_retains_failure_or_verified_retry(tmp_path: Path, monkeypatch, failure):
    output = tmp_path / "drill"
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "lakehouse-ops-owner-exit-test")
    monkeypatch.setattr(sys, "argv", ["drill", "--output", str(output),
                                    "--s3-endpoint-url", "unused", "--server", "unused"])
    monkeypatch.setattr(drill.boto3, "client", lambda *a, **kw: object())
    monkeypatch.setattr(drill, "generate_commerce_fixture", lambda *a: SimpleNamespace(
        path=output / "fixture", batch_id="a" * 16,
    ))
    monkeypatch.setattr(drill, "CommerceS3LandingZone", lambda *a, **kw: SimpleNamespace(
        write=lambda *_: None,
    ))
    monkeypatch.setattr(drill, "CommerceBatchPlanner", lambda *a, **kw: object())

    class Trino:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def query(self, sql):
            return []

    monkeypatch.setattr(drill, "TrinoClient", Trino)
    count = 0

    def audit(*_):
        nonlocal count
        count += 1
        if count == 1:
            return [{}] if failure == "existing" else []
        return [{"table": "gold", "rows": 99 if failure == "files" and count == 3 else 100,
                 "snapshots": count}]

    monkeypatch.setattr(drill, "audit_tables", audit)

    def kill(*a, **kw):
        journal = CommerceRunJournal(output / "journal.sqlite3")
        journal.record("wrong" if failure == "journal" else drill.COMMERCE_STAGES[-1], "started")
        if failure == "checkpoint":
            (output / "checkpoint.json").write_text("{}")
        return -9

    monkeypatch.setattr(drill, "force_owner_exit", kill)

    def run(planner, query, **kwargs):
        if "journal" in kwargs:
            kwargs["journal"].record("run", "ready")
            return {"status": "ready", "batch_id": "a" * 16}
        return {"status": "ready" if failure == "idle" else "idle"}

    monkeypatch.setattr(drill, "run_commerce_batch", run)
    monkeypatch.setattr(
        drill, "commerce_runner_lock", lambda: commerce_runner_lock(tmp_path / "lock"),
    )
    assert drill.main() == (2 if failure else 0)
    report = json.loads((output / "owner-exit.json").read_text())
    assert report["status"] == ("failed" if failure else "ready")
    if not failure:
        assert report["checkpoint_before_retry"] == "absent"
        assert report["owner_exit_code"] == -9
