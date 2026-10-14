from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from lakehouse_ops.commerce_change_scenario import generate_change_scenario
from lakehouse_ops.commerce_history_correction import (
    CORRECTED_HISTORY,
    HISTORY_STAGE,
    exercise_history_correction,
    generate_following_observation,
    generate_late_observation,
)
from lakehouse_ops.commerce_journal_inspection import inspect_journal
from lakehouse_ops.ingestion.commerce_s3_landing import _load_fixture


def test_late_fixture_has_hash_bound_earlier_effective_time(tmp_path: Path):
    scenario = generate_change_scenario(tmp_path / "scenario")
    initial = Path(scenario["batches"][0]["path"])
    first = generate_late_observation(initial, tmp_path / "first")
    second = generate_late_observation(initial, tmp_path / "second")
    assert first == second
    assert first["batch_at"] == "2026-02-01T12:00:00Z"
    assert first["batch_id"] not in [item["batch_id"] for item in scenario["batches"]]
    manifest, objects, body = _load_fixture(tmp_path / "first")
    assert len(objects) == 4
    assert hashlib.sha256(body).hexdigest() == first["manifest_sha256"]
    assert manifest["batch_id"] == first["batch_id"]
    customer = json.loads((tmp_path / "first/customers.jsonl").read_text())
    assert customer["full_name"] == "Alice Corrected"
    assert json.loads((initial / "customers.jsonl").read_text())["full_name"] == "Alice"
    with pytest.raises(FileExistsError):
        generate_late_observation(initial, tmp_path / "first")


def test_following_fixture_is_newer_hash_bound_and_preserves_latest_state(tmp_path: Path):
    scenario = generate_change_scenario(tmp_path / "scenario")
    latest = Path(scenario["batches"][-1]["path"])
    before = {path.name: path.read_bytes() for path in latest.iterdir()}
    first = generate_following_observation(latest, tmp_path / "following")
    second = generate_following_observation(latest, tmp_path / "repeated")
    assert first == second
    assert first["batch_at"] == "2026-02-04T00:00:00Z"
    assert first["batch_id"] not in [item["batch_id"] for item in scenario["batches"]]
    manifest, objects, body = _load_fixture(tmp_path / "following")
    assert manifest["config"]["source_semantics"] == "full_snapshot"
    assert len(objects) == 4
    assert hashlib.sha256(body).hexdigest() == first["manifest_sha256"]
    for table, item in manifest["tables"].items():
        original = [json.loads(line) for line in (latest / item["file"]).read_text().splitlines()]
        following = [json.loads(line) for line in (
            tmp_path / "following" / item["file"]
        ).read_text().splitlines()]
        assert following == original, table
    assert {path.name: path.read_bytes() for path in latest.iterdir()} == before


@pytest.mark.parametrize("failure", [None, "stage", "oracle", "revenue", "checkpoint",
                                     "snapshot_missing", "replay_snapshot", "current_snapshot"])
def test_correction_and_replay_preserve_revenue_checkpoint_and_evidence(tmp_path, failure):
    generate_change_scenario(tmp_path / "scenario")
    expected = json.loads((tmp_path / "scenario/expected.json").read_text())
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_bytes(b"original checkpoint\n")
    batch = {"batch_id": "e" * 16, "manifest_sha256": "a" * 64,
             "batch_at": "2026-02-01T12:00:00Z"}
    stages = []
    rebuilds = 0
    report = {}

    def stage(service, selected, *, journal, history_mode):
        nonlocal rebuilds
        assert selected == batch and journal.batch == batch
        assert history_mode == ("rebuild" if service == HISTORY_STAGE else "forward")
        stages.append((service, journal.run_id))
        if failure == "stage":
            raise RuntimeError("injected Spark failure")
        if service == HISTORY_STAGE:
            rebuilds += 1
            if failure == "checkpoint":
                checkpoint.write_bytes(b"unexpected advance")

    def query(sql):
        if "dim_customers_scd2$snapshots" in sql:
            return [{"snapshots": 3 + (int(bool(rebuilds)) if failure != "snapshot_missing" else 0)
                     + int(failure == "replay_snapshot" and rebuilds == 2)}]
        if "commerce_current_daily$snapshots" in sql:
            return [{"snapshots": 3 + int(failure == "current_snapshot" and bool(rebuilds))}]
        if "commerce_current_daily" in sql:
            return [{"order_day": day, "captured_revenue_cents": 0
                     if failure == "revenue" and rebuilds else value,
                     "order_count": 1, "source_batch_id": expected["batches"][-1]["batch_id"]}
                    for day, value in expected["latest_daily_revenue_cents"].items()]
        if "dim_customers_scd2" in sql:
            history = CORRECTED_HISTORY if rebuilds else expected["customer_history"]
            return [{**row, "customer_id": "C00000001", "is_current": row["valid_to"] is None,
                     "full_name": "wrong" if failure == "oracle" and rebuilds else row["full_name"]}
                    for row in history]
        if "SELECT order_id" in sql:
            return expected["latest_orders"]
        pytest.fail(sql)

    if failure:
        with pytest.raises((ValueError, RuntimeError)):
            exercise_history_correction(query, stage, batch, expected, tmp_path, report)
        assert report["late_customer_correction"]["status"] == "failed"
    else:
        exercise_history_correction(query, stage, batch, expected, tmp_path, report)
        sample = report["late_customer_correction"]
        assert sample["status"] == "ready" and sample["replay"] == "snapshot_stable"
        assert sample["oracle"]["customer_history"] == CORRECTED_HISTORY
        assert sample["oracle"]["captured_revenue_cents"] == 2200
        assert [service for service, _ in stages] == ["bronze-input-sync", "spark-commerce-bronze",
                                                     "spark-commerce-customer-silver",
                                                     HISTORY_STAGE, HISTORY_STAGE]
        assert len({run for _, run in stages}) == 2
    attempts = inspect_journal(tmp_path / "run-journal.sqlite3")["runs"]
    assert all(attempt["checkpoint_completed_events"] == 0 for attempt in attempts)
    assert sum(attempt["outcome"] == "failed" for attempt in attempts) == bool(failure)
    if failure != "checkpoint":
        assert checkpoint.read_bytes() == b"original checkpoint\n"


@pytest.mark.parametrize("previous", [None, "forward"])
@pytest.mark.parametrize("failure", [False, True])
def test_opt_in_driver_restores_history_mode_after_stage(tmp_path, monkeypatch, previous, failure):
    from lakehouse_ops import commerce_history_correction as correction
    from lakehouse_ops import commerce_linked_acceptance as linked

    if previous is None:
        monkeypatch.delenv("COMMERCE_CUSTOMER_HISTORY_MODE", raising=False)
    else:
        monkeypatch.setenv("COMMERCE_CUSTOMER_HISTORY_MODE", previous)
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "lakehouse-ops-linked-unit")
    monkeypatch.setattr("sys.argv", ["linked", "--output", str(tmp_path / "proof"),
                                    "--server", "unused", "--s3-endpoint-url", "unused",
                                    "--late-customer-history"])

    class Trino:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def query(self, sql):
            return []

    monkeypatch.setattr(linked, "TrinoClient", Trino)
    monkeypatch.setattr(linked.boto3, "client", lambda *a, **kw: object())
    monkeypatch.setattr(linked, "CommerceBatchPlanner", lambda *a, **kw: object())
    monkeypatch.setattr(linked, "commerce_runner_lock", lambda: Trino())
    monkeypatch.setattr(linked, "CommerceS3LandingZone", lambda *a, **kw: SimpleNamespace(
        write=lambda _: SimpleNamespace(created=0, as_dict=lambda: {"created": 0}),
    ))
    monkeypatch.setattr(linked, "exercise_linked_batches",
                        lambda *a: a[-1].update(status="ready"))
    def completed(*a, **kw):
        if "journal" in kw:
            manifest = json.loads((tmp_path / "proof/late-observation/manifest.json").read_text())
            return {"status": "ready", "batch_id": manifest["batch_id"]}
        return {"status": "idle", "batch_id": None}

    monkeypatch.setattr(linked, "run_commerce_batch", completed)
    modes = []

    def compose(*a, **kw):
        modes.append(os.environ.get("COMMERCE_CUSTOMER_HISTORY_MODE"))
        if failure:
            raise RuntimeError("injected mode-stage failure")

    def exercise(query, run, batch, expected, output, report):
        run(HISTORY_STAGE, batch, journal=object(), history_mode="rebuild")
        report["late_customer_correction"] = {"status": "ready"}

    monkeypatch.setattr(linked, "run_compose_stage", compose)
    monkeypatch.setattr(correction, "exercise_history_correction", exercise)
    completions = []
    def late_completion(planner, query, run, batch, expected, output, report):
        completions.append(batch["batch_id"])
        report["late_batch_completion"] = {"status": "ready", "batch_id": batch["batch_id"]}

    monkeypatch.setattr(correction, "exercise_late_batch_completion", late_completion)
    monkeypatch.setattr(correction, "exercise_following_batch", lambda *args: args[-1].update(
        next_batch_after_correction={"status": "ready"}, idle_after_next_batch={"status": "idle"},
    ))
    assert linked.main() == (2 if failure else 0)
    assert modes == ["rebuild"]
    assert len(completions) == (0 if failure else 1)
    assert os.environ.get("COMMERCE_CUSTOMER_HISTORY_MODE") == previous
    report = json.loads((tmp_path / "proof/linked.json").read_text())
    assert report["status"] == ("failed" if failure else "ready")


@pytest.mark.parametrize("failure", [None, "metadata_snapshot", "stage", "retry_stage",
                                     "checkpoint", "mode", "identity", "oracle", "batch_gold",
                                     "replay_snapshot",
                                     "history_snapshot", "current_snapshot", "row_hash",
                                     "lost_snapshot"])
def test_late_full_snapshot_failure_retries_before_checkpoint(tmp_path, failure):
    from lakehouse_ops.commerce_history_correction import exercise_late_batch_completion
    from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES, CURRENT_STAGE

    generate_change_scenario(tmp_path / "scenario")
    expected = json.loads((tmp_path / "scenario/expected.json").read_text())
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_bytes(b"three completed original batches\n")
    batch = {"batch_id": "e" * 16, "manifest_sha256": "a" * 64,
             "batch_at": "2026-02-01T12:00:00Z"}
    pending = True
    stages, commits, owners = [], [], []
    gold_writes = 0
    report = {}

    class Planner:
        def plan(self, **kwargs):
            selected = {**batch, "batch_id": "f" * 16} if failure == "identity" else batch
            return {"batches": [selected] if pending else []}

        def execution_options(self, selected):
            return {"customer_history_mode": "forward" if failure == "mode" else "rebuild"}

        def commit(self, selected_id, **kwargs):
            nonlocal pending
            assert stages == [*COMMERCE_STAGES, *COMMERCE_STAGES, CURRENT_STAGE]
            assert not commits and gold_writes == 2
            assert checkpoint.read_bytes() == b"three completed original batches\n"
            assert kwargs["expected_manifest_sha256"] == batch["manifest_sha256"]
            commits.append(selected_id)
            checkpoint.write_bytes(b"four completed full snapshots\n")
            pending = False
            return {"created": True}

    def stage(service, selected, *, journal):
        nonlocal gold_writes
        assert selected["batch_id"] == batch["batch_id"]
        assert selected["customer_history_mode"] == "rebuild"
        assert journal.batch == selected and not commits
        stages.append(service)
        owners.append(journal.run_id)
        if failure == "stage":
            raise RuntimeError("unexpected real stage failure, not the drill injection")
        if failure == "retry_stage" and gold_writes == 1:
            raise RuntimeError("unexpected retry stage failure")
        if service == COMMERCE_STAGES[-1]:
            gold_writes += 1
            if failure == "checkpoint":
                checkpoint.write_bytes(b"unexpected partial checkpoint\n")

    def query(sql):
        if "information_schema.tables" in sql:
            return [{"table_schema": "gold", "table_name": name} for name in (
                "commerce_daily", "commerce_current_daily", "dim_customers_scd2",
            )]
        if "$files" in sql:
            changed = failure == "replay_snapshot" and gold_writes == 2
            return [{"content": 0, "file_path": "changed" if changed else "stable.parquet",
                     "record_count": 1, "file_size_in_bytes": 100}]
        if "SELECT *" in sql:
            return [{"value": "changed" if failure == "row_hash" and gold_writes == 2 else "same"}]
        if "commerce_daily$snapshots" in sql:
            return [{"snapshots": 3 + int(bool(gold_writes))
                     + int(failure in ("replay_snapshot", "metadata_snapshot") and gold_writes == 2)
                     - int(failure == "lost_snapshot" and gold_writes == 2)}]
        if "$snapshots" in sql:
            return [{"snapshots": (4 + int(failure == "history_snapshot" and bool(gold_writes))
                                    if "dim_customers_scd2" in sql else
                                    3 + int(failure == "current_snapshot" and bool(gold_writes)))}]
        if "commerce_current_daily" in sql:
            return [{"order_day": day, "captured_revenue_cents": value,
                     "order_count": 1, "source_batch_id": expected["batches"][-1]["batch_id"]}
                    for day, value in expected["latest_daily_revenue_cents"].items()]
        if "dim_customers_scd2" in sql:
            return [{**row, "customer_id": "C00000001", "is_current": row["valid_to"] is None,
                     "full_name": "wrong" if failure == "oracle" else row["full_name"]}
                    for row in CORRECTED_HISTORY]
        if "SELECT order_id" in sql:
            return expected["latest_orders"]
        return [{"days": 1, "orders": 1, "captured_revenue_cents": (
            1100 if failure == "batch_gold" else 1000
        ), "invalid_days": 0}]

    if failure not in (None, "metadata_snapshot"):
        with pytest.raises((ValueError, RuntimeError)):
            exercise_late_batch_completion(Planner(), query, stage, batch,
                                           expected, tmp_path, report)
        assert not commits and pending
        assert report["late_batch_recovery"]["status"] == "failed"
        assert "late_batch_completion" not in report
    else:
        exercise_late_batch_completion(Planner(), query, stage, batch,
                                       expected, tmp_path, report)
        assert commits == [batch["batch_id"]] and not pending
        recovery = report["late_batch_recovery"]
        assert recovery["status"] == "ready"
        assert recovery["checkpoint_after_failure"] == "unchanged"
        assert recovery["batch_gold_replay"] == "rows_and_files_stable"
        before = recovery["published_after_failure"]
        after = recovery["published_before_retry_checkpoint"]
        assert [{k: v for k, v in row.items() if k != "snapshots"} for row in before] == [
            {k: v for k, v in row.items() if k != "snapshots"} for row in after
        ]
        assert recovery["failed_run_id"] != recovery["retry_run_id"]
        assert owners == ([recovery["failed_run_id"]] * len(COMMERCE_STAGES)
                          + [recovery["retry_run_id"]] * (len(COMMERCE_STAGES) + 1))
        assert report["late_batch_completion"]["completed_stages"] == [
            *COMMERCE_STAGES, CURRENT_STAGE,
        ]
        runs = inspect_journal(tmp_path / "run-journal.sqlite3")["runs"]
        assert len(runs) == 2
        failed = next(run for run in runs if run["run_id"] == recovery["failed_run_id"])
        retry = next(run for run in runs if run["run_id"] == recovery["retry_run_id"])
        assert failed["outcome"] == "failed" and failed["checkpoint_completed_events"] == 0
        assert failed["phase"] == COMMERCE_STAGES[-1]
        assert retry["outcome"] == "ready" and retry["checkpoint_completed_events"] == 1
