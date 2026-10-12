from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from lakehouse_ops.commerce_journal_inspection import inspect_journal, main
from lakehouse_ops.commerce_run_journal import CommerceJournalError, CommerceRunJournal


def test_recent_attempts_preserve_identity_and_do_not_modify_journal(tmp_path: Path) -> None:
    path = tmp_path / "journal space #.sqlite3"
    batch = {"batch_id": "a" * 16, "manifest_sha256": "b" * 64}
    journals = [CommerceRunJournal(path) for _ in range(4)]
    for journal, terminal in zip(journals, ["ready", "idle", "failed", None], strict=True):
        journal.record("planning", "selected", batch=batch)
        if terminal == "ready":
            journal.record("checkpoint", "completed")
        if terminal is not None:
            journal.record("run" if terminal != "failed" else "verification", terminal,
                           error_type="TrinoQueryError" if terminal == "failed" else None)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    report = inspect_journal(path)
    assert report["scope"] == "diagnostic_journal_not_checkpoint_or_liveness"
    assert [r["outcome"] for r in report["runs"]] == ["unfinished", "failed", "idle", "ready"]
    assert [r["run_id"] for r in report["runs"]] == [j.run_id for j in reversed(journals)]
    assert report["runs"][-1]["checkpoint_completed_events"] == 1
    assert report["runs"][-1]["event_count"] == 3
    assert report["runs"][1]["error_type"] == "TrinoQueryError"
    assert all(r["manifest_sha256"] == batch["manifest_sha256"] for r in report["runs"])
    assert inspect_journal(path, limit=1)["runs"] == report["runs"][:1]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


@pytest.mark.parametrize("limit", [0, 101, True, 1.5])
def test_bad_limit_does_not_create_journal(tmp_path: Path, limit) -> None:
    path = tmp_path / "missing.sqlite3"
    with pytest.raises(ValueError, match="between 1 and 100"):
        inspect_journal(path, limit=limit)
    assert not path.exists()


@pytest.mark.parametrize("kind", ["missing", "corrupt", "wrong_schema"])
def test_unreadable_journal_fails_without_repair(tmp_path: Path, kind: str) -> None:
    path = tmp_path / "journal.sqlite3"
    if kind == "corrupt":
        path.write_bytes(b"not a database")
    elif kind == "wrong_schema":
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE unrelated(value TEXT)")
    original = path.read_bytes() if path.exists() else None
    with pytest.raises(CommerceJournalError, match="cannot inspect"):
        inspect_journal(path)
    assert (path.read_bytes() if path.exists() else None) == original


def test_checkpoint_event_alone_is_unfinished_not_success(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite3"
    journal = CommerceRunJournal(path)
    journal.record("checkpoint", "completed")
    result = inspect_journal(path)["runs"][0]
    assert result["outcome"] == "unfinished"
    assert result["checkpoint_completed_events"] == 1


def test_empty_events_and_extra_tables_are_supported(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite3"
    CommerceRunJournal(path).record("run", "idle")
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE commerce_container_events(value TEXT)")
    assert inspect_journal(path)["runs"][0]["outcome"] == "idle"
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM commerce_run_events")
    assert inspect_journal(path)["runs"] == []


def test_module_cli_success_and_missing_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture,
) -> None:
    path = tmp_path / "journal.sqlite3"
    CommerceRunJournal(path).record("run", "idle")
    assert main(["--journal", str(path), "--limit", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["runs"][0]["outcome"] == "idle"
    assert main(["--journal", str(tmp_path / "missing.sqlite3")]) == 2
    output = capsys.readouterr()
    assert not output.out
    assert "inspection failed" in output.err
