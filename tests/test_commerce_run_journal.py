from __future__ import annotations

import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES, run_commerce_batch
from lakehouse_ops.commerce_run_journal import CommerceJournalError, CommerceRunJournal

BATCH = {"batch_id": "a" * 16, "manifest_sha256": "b" * 64}


class Planner:
    def __init__(self) -> None:
        self.commits: list[str] = []

    def plan(self, **kwargs: Any) -> dict[str, Any]:
        return {"batches": [] if self.commits else [BATCH]}

    def commit(self, batch_id: str, **kwargs: Any) -> dict[str, Any]:
        self.commits.append(batch_id)
        return {"created": True}


def gold(_: str) -> list[dict[str, int]]:
    return [{"days": 1, "orders": 1, "captured_revenue_cents": 100, "invalid_days": 0}]


def events(path: Path) -> list[dict[str, Any]]:
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(row) for row in connection.execute(
            "SELECT * FROM commerce_run_events ORDER BY sequence"
        )]


def test_journal_orders_stage_verification_and_checkpoint_then_records_idle(tmp_path: Path) -> None:
    path = tmp_path / "state.runs.sqlite3"
    journal = CommerceRunJournal(path)
    planner = Planner()

    def execute(service: str, batch: dict[str, str]) -> None:
        assert events(path)[-1]["phase"] == service
        assert events(path)[-1]["event"] == "started"
        assert planner.commits == []

    report = run_commerce_batch(planner, gold, run_stage=execute, journal=journal)
    assert report["status"] == "ready"
    rows = events(path)
    assert [(row["phase"], row["event"]) for row in rows] == [
        ("planning", "started"), ("planning", "selected"),
        *[(stage, event) for stage in COMMERCE_STAGES for event in ("started", "completed")],
        ("verification", "started"), ("verification", "completed"),
        ("checkpoint", "started"), ("checkpoint", "completed"), ("run", "ready"),
    ]
    assert all(row["run_id"] == journal.run_id for row in rows)
    assert all(row["batch_id"] == BATCH["batch_id"] for row in rows[1:])
    assert all(row["manifest_sha256"] == BATCH["manifest_sha256"] for row in rows[1:])
    assert all(row["recorded_at"].endswith("+00:00") for row in rows)
    idle = CommerceRunJournal(path)
    run_commerce_batch(planner, gold, run_stage=execute, journal=idle)
    assert events(path)[:len(rows)] == rows
    assert [(row["event"], row["batch_id"]) for row in events(path)[len(rows):]] == [
        ("started", None), ("idle", None),
    ]


@pytest.mark.parametrize("phase", ["planning", *COMMERCE_STAGES, "verification", "checkpoint"])
def test_failure_is_retained_without_premature_checkpoint(
    tmp_path: Path, phase: str,
) -> None:
    path = tmp_path / "journal.sqlite3"
    planner = Planner()

    def fail() -> None:
        raise RuntimeError("secret must not enter the journal")

    class FailingPlanner(Planner):
        def plan(self, **kwargs: Any) -> dict[str, Any]:
            if phase == "planning":
                fail()
            return planner.plan(**kwargs)

        def commit(self, batch_id: str, **kwargs: Any) -> dict[str, Any]:
            if phase == "checkpoint":
                fail()
            return planner.commit(batch_id, **kwargs)

    def execute(service: str, batch: dict[str, str]) -> None:
        if phase == service:
            fail()

    def query(sql: str) -> list[dict[str, int]]:
        if phase == "verification":
            fail()
        return gold(sql)

    with pytest.raises(RuntimeError, match="secret"):
        run_commerce_batch(FailingPlanner(), query, run_stage=execute,
                           journal=CommerceRunJournal(path))
    assert planner.commits == []
    rows = events(path)
    assert rows[-1]["phase"] == phase
    assert rows[-1]["event"] == "failed"
    assert rows[-1]["error_type"] == "RuntimeError"
    assert "secret" not in str(rows)
    assert not any(row["event"] == "ready" for row in rows)


def test_real_process_exit_retains_incomplete_attempt_and_retry_appends(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite3"
    program = """
import os, sys
from pathlib import Path
from lakehouse_ops.commerce_run_journal import CommerceRunJournal
journal = CommerceRunJournal(Path(sys.argv[1]))
journal.record('planning', 'started')
journal.record('spark-commerce-bronze', 'started', batch={
    'batch_id': 'a' * 16, 'manifest_sha256': 'b' * 64,
})
os._exit(9)
"""
    result = subprocess.run([sys.executable, "-c", program, str(path)], check=False, timeout=10)
    assert result.returncode == 9
    previous = events(path)
    assert len(previous) == 2
    assert previous[-1]["event"] == "started"
    planner = Planner()
    retry = CommerceRunJournal(path)
    run_commerce_batch(planner, gold, run_stage=lambda *_: None, journal=retry)
    assert events(path)[:2] == previous
    assert events(path)[-1]["run_id"] != previous[-1]["run_id"]
    assert events(path)[-1]["event"] == "ready"
    assert planner.commits == [BATCH["batch_id"]]


def test_corrupt_journal_fails_before_planning(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite3"
    path.write_bytes(b"not sqlite")
    with pytest.raises(CommerceJournalError, match="journal write failed"):
        run_commerce_batch(
            Planner(), lambda _: pytest.fail("must not query"),
            run_stage=lambda *_: pytest.fail("must not compute"), journal=CommerceRunJournal(path),
        )
    assert path.read_bytes() == b"not sqlite"


@pytest.mark.parametrize("event", ["started", "completed"])
def test_checkpoint_journal_failure_preserves_authoritative_checkpoint_semantics(
    tmp_path: Path, event: str,
) -> None:
    class UnwritableJournal(CommerceRunJournal):
        def record(self, phase: str, status: str, **kwargs: Any) -> None:
            if phase == "checkpoint" and status == event:
                raise CommerceJournalError("journal storage unavailable")
            super().record(phase, status, **kwargs)

    planner = Planner()
    with pytest.raises(CommerceJournalError):
        run_commerce_batch(
            planner, gold, run_stage=lambda *_: None,
            journal=UnwritableJournal(tmp_path / "journal.sqlite3"),
        )
    assert planner.commits == ([] if event == "started" else [BATCH["batch_id"]])


def test_keyboard_interrupt_is_retained(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite3"

    def interrupt(*_: Any) -> None:
        raise KeyboardInterrupt

    planner = Planner()
    with pytest.raises(KeyboardInterrupt):
        run_commerce_batch(planner, gold, run_stage=interrupt, journal=CommerceRunJournal(path))
    assert events(path)[-1]["error_type"] == "KeyboardInterrupt"
    assert events(path)[-1]["event"] == "failed"
    assert planner.commits == []
