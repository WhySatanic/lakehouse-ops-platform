from __future__ import annotations

from pathlib import Path

import pytest

from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES
from lakehouse_ops.commerce_recovery import audit_tables, exercise_recovery


class Planner:
    def __init__(self, state: Path):
        self.state = state
        self.committed = False

    def plan(self, **kwargs):
        return {"batches": [] if self.committed else [
            {"batch_id": "a" * 16, "manifest_sha256": "b" * 64},
        ]}

    def commit(self, batch_id, **kwargs):
        self.committed = True
        self.state.write_text("checkpoint")
        return {"created": True}


@pytest.mark.parametrize("changed", [False, True])
def test_interrupt_then_retry_preserves_published_audit(tmp_path: Path, changed: bool):
    state = tmp_path / "checkpoint.json"
    planner = Planner(state)
    stages = []
    queries = []
    audit_calls = 0

    def stage(service, batch):
        stages.append(service)
        assert not state.exists()

    def query(sql):
        assert len(stages) == 2 * len(COMMERCE_STAGES)
        assert not state.exists()
        queries.append(sql)
        return [{"days": 1, "orders": 1, "captured_revenue_cents": 100, "invalid_days": 0}]

    def audit():
        nonlocal audit_calls
        audit_calls += 1
        if audit_calls == 1:
            assert not state.exists()
            assert not queries
        else:
            assert state.exists()
        return [{"rows": 2 if changed and audit_calls == 2 else 1, "snapshots": 1}]

    if changed:
        with pytest.raises(ValueError, match="changed published"):
            exercise_recovery(planner, query, stage, audit, state)
    else:
        report = exercise_recovery(planner, query, stage, audit, state)
        assert report["status"] == "ready"
        assert report["checkpoint_before_retry"] == "absent"
        assert report["published_before_retry"] == report["published_after_retry"]
        assert report["idle"]["status"] == "idle"
    assert stages == list(COMMERCE_STAGES) * 2
    assert len(queries) == 1


def test_existing_checkpoint_rejected_before_compute(tmp_path: Path):
    state = tmp_path / "checkpoint.json"
    state.write_text("existing")
    with pytest.raises(ValueError, match="fresh checkpoint"):
        exercise_recovery(Planner(state), lambda _: [], lambda *_: None, lambda: [], state)
    assert state.read_text() == "existing"


def test_unexpected_compute_failure_is_not_the_injected_success(tmp_path: Path):
    state = tmp_path / "checkpoint.json"

    def fail(*args):
        raise RuntimeError("real stage failure")

    with pytest.raises(RuntimeError, match="real stage failure"):
        exercise_recovery(Planner(state), lambda _: [], fail, lambda: [], state)
    assert not state.exists()


def test_failed_retry_retains_baseline_without_checkpoint(tmp_path: Path):
    state = tmp_path / "checkpoint.json"
    calls = 0
    evidence = {}

    def stage(*args):
        nonlocal calls
        calls += 1
        if calls > len(COMMERCE_STAGES):
            raise RuntimeError("retry compute failed")

    baseline = [{"rows": 1, "snapshots": 1}]
    with pytest.raises(RuntimeError, match="retry compute failed"):
        exercise_recovery(Planner(state), lambda _: [], stage, lambda: baseline, state,
                          evidence=evidence)
    assert evidence["published_before_retry"] == baseline
    assert "published_after_retry" not in evidence
    assert not state.exists()


def test_main_requires_scoped_project_before_creating_artifacts(tmp_path: Path, monkeypatch):
    from lakehouse_ops.commerce_recovery import main

    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "unrelated")
    monkeypatch.setattr("sys.argv", ["recovery", "--output", str(tmp_path / "evidence"),
                                    "--s3-endpoint-url", "http://localhost:19300",
                                    "--server", "http://localhost:18300"])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2
    assert not (tmp_path / "evidence").exists()


def test_audit_content_hash_is_independent_of_physical_row_order():
    reverse = False

    def query(sql):
        if "information_schema" in sql:
            return [{"table_schema": "gold", "table_name": "commerce_daily"}]
        if "$snapshots" in sql:
            return [{"snapshots": 2}]
        if "$files" in sql:
            return [{"content": 0, "file_path": "a.parquet", "record_count": 2,
                     "file_size_in_bytes": 100}]
        return [{"id": 2}, {"id": 1}] if reverse else [{"id": 1}, {"id": 2}]

    before = audit_tables(query)
    reverse = True
    assert audit_tables(query) == before
    assert before[0]["rows"] == 2
    assert before[0]["snapshots"] == 2


@pytest.mark.parametrize("change", ["empty_snapshot", "file", "lost_snapshot", "row_hash"])
def test_recovery_accepts_only_metadata_only_snapshot_growth(tmp_path: Path, change: str):
    state = tmp_path / "checkpoint.json"
    calls = 0
    evidence = {}

    def audit():
        nonlocal calls
        calls += 1
        return [{"table": "gold.dim_customers_scd2", "rows": 10,
                 "rows_sha256": "different" if change == "row_hash" and calls == 2 else "same",
                 "files": 1,
                 "files_sha256": "different" if change == "file" and calls == 2 else "same",
                 "snapshots": (0 if change == "lost_snapshot" else 2) if calls == 2 else 1}]

    def query(_):
        return [{"days": 1, "orders": 1, "captured_revenue_cents": 100, "invalid_days": 0}]
    if change == "empty_snapshot":
        result = exercise_recovery(Planner(state), query, lambda *_: None, audit, state,
                                   evidence=evidence)
        assert result["idle"]["status"] == "idle"
    else:
        with pytest.raises(ValueError):
            exercise_recovery(Planner(state), query, lambda *_: None, audit, state,
                              evidence=evidence)
    assert evidence["published_before_retry"][0]["snapshots"] == 1
    assert "published_after_retry" in evidence
