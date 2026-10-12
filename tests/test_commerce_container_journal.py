from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from lakehouse_ops import commerce_pipeline as pipeline
from lakehouse_ops.commerce_run_journal import CommerceJournalError, CommerceRunJournal

BATCH = {"batch_id": "a" * 16, "manifest_sha256": "b" * 64}


@pytest.mark.parametrize("result", ["ready", "failed", "timeout", "cleanup"])
def test_durable_intent_precedes_docker_and_tracks_exact_container(
    tmp_path: Path, monkeypatch, result,
):
    journal = CommerceRunJournal(tmp_path / "journal.sqlite3")
    journal.record("planning", "selected", batch=BATCH)
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "owned-test")
    names = []

    def run(command, **kwargs):
        if command[:2] == ["docker", "compose"]:
            name = command[command.index("--name") + 1]
            names.append(name)
            with sqlite3.connect(journal.path) as db:
                row = db.execute(
                    "SELECT run_id,container_name,batch_id,manifest_sha256,event,project_hint "
                    "FROM commerce_container_events ORDER BY sequence DESC LIMIT 1"
                ).fetchone()
            assert row == (journal.run_id, name, BATCH["batch_id"],
                           BATCH["manifest_sha256"], "launch_intent", "owned-test")
            assert f"lakehouse.ops.run_id={journal.run_id}" in command
            assert f"lakehouse.ops.batch_id={BATCH['batch_id']}" in command
            if result == "failed":
                raise subprocess.CalledProcessError(1, command)
            if result in {"timeout", "cleanup"}:
                raise subprocess.TimeoutExpired(command, 1)
        else:
            assert command == ["docker", "rm", "--force", names[0]]
            if result == "cleanup":
                raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(pipeline.subprocess, "run", run)
    if result == "ready":
        pipeline.run_compose_stage("stage", BATCH, bucket="unused", journal=journal)
    else:
        with pytest.raises(pipeline.CommercePipelineError):
            pipeline.run_compose_stage("stage", BATCH, bucket="unused", journal=journal)
    expected = {"ready": "completed", "failed": "failed", "timeout": "timeout_removed",
                "cleanup": "cleanup_unconfirmed"}[result]
    with sqlite3.connect(journal.path) as db:
        assert db.execute(
            "SELECT event FROM commerce_container_events ORDER BY sequence"
        ).fetchall() == [
            ("launch_intent",), (expected,),
        ]
        assert db.execute("SELECT phase,event FROM commerce_run_events").fetchall() == [
            ("planning", "selected"),
        ]
    assert journal.phase == "planning"


def test_corrupt_ownership_journal_prevents_docker(tmp_path: Path, monkeypatch):
    path = tmp_path / "journal.sqlite3"
    path.write_bytes(b"corrupt journal")
    monkeypatch.setattr(pipeline.subprocess, "run", lambda *a, **kw: pytest.fail("must not start"))
    with pytest.raises(CommerceJournalError):
        pipeline.run_compose_stage(
            "stage", BATCH, bucket="unused", journal=CommerceRunJournal(path),
        )
    assert path.read_bytes() == b"corrupt journal"


def test_post_execution_journal_failure_does_not_claim_success(tmp_path: Path, monkeypatch):
    class Journal(CommerceRunJournal):
        def record_container(self, service, container, event, **kwargs):
            if event == "completed":
                raise CommerceJournalError("disk unavailable")
            super().record_container(service, container, event, **kwargs)

    monkeypatch.setattr(pipeline.subprocess, "run", lambda *a, **kw: None)
    journal = Journal(tmp_path / "journal.sqlite3")
    planner = SimpleNamespace(
        plan=lambda **kw: {"batches": [BATCH]},
        commit=lambda *a, **kw: pytest.fail("must not checkpoint"),
    )
    with pytest.raises(CommerceJournalError):
        pipeline.run_commerce_batch(
            planner, lambda *_: pytest.fail("must not verify"), journal=journal,
            run_stage=lambda service, batch: pipeline.run_compose_stage(
                service, batch, bucket="unused", journal=journal,
            ),
        )
