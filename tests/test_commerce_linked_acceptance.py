from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from test_commerce_batches import FakeS3Client
from test_commerce_late_queue import add_snapshot

from lakehouse_ops.commerce_change_scenario import generate_change_scenario
from lakehouse_ops.commerce_journal_inspection import inspect_journal
from lakehouse_ops.commerce_linked_acceptance import (
    CURRENT_STAGE,
    exercise_inflight_arrival,
    exercise_linked_batches,
    utc_timestamp,
)
from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES
from lakehouse_ops.ingestion.commerce_batches import CommerceBatchPlanner


@pytest.mark.parametrize("failure", [None, "oracle", "current_stage", "replay_snapshot",
                                     "revenue", "orders", "current_flag", "replay_stage",
                                     "ownership"])
def test_linked_snapshot_oracles_precede_checkpoint(tmp_path: Path, failure: str | None):
    generate_change_scenario(tmp_path / "scenario")
    expected = json.loads((tmp_path / "scenario" / "expected.json").read_text())
    batches = [{**item, "manifest_sha256": "a" * 64} for item in expected["batches"]]
    commits = []
    stages = []
    index = 0
    current_calls = 0
    owners = []

    class Planner:
        def execution_options(self, selected):
            return {"customer_history_mode": "forward"}

        def plan(self, **kwargs):
            return {"batches": batches[len(commits):][:kwargs["max_batches"]]}

        def commit(self, batch_id, **kwargs):
            assert stages[-1] == CURRENT_STAGE
            assert index == len(commits)
            assert report["samples"][-1]["oracle"]["batch_id"] == batch_id
            commits.append(batch_id)
            return {"created": True}

    def stage(service, batch, *, journal=None):
        nonlocal index, current_calls
        assert journal is not None, "linked stages must retain container ownership"
        assert journal.batch == batch
        index = next(i for i, item in enumerate(batches) if item["batch_id"] == batch["batch_id"])
        owners.append((journal.run_id, batch["batch_id"]))
        journal.record_container(service, f"owned-{len(owners)}", "launch_intent",
                                 batch=batch, project_hint="linked-test")
        if failure == "ownership" and index == 1:
            raise OSError("ownership journal failed")
        stages.append(service)
        if service == CURRENT_STAGE:
            current_calls += 1
            if failure == "current_stage" and index == 1:
                raise RuntimeError("current publication failed")
            if failure == "replay_stage" and current_calls == 4:
                raise RuntimeError("replay publication failed")

    def query(sql):
        if "$snapshots" in sql:
            return [{"snapshots": 4 if failure == "replay_snapshot" and current_calls == 4 else 3}]
        if "commerce_current_daily" in sql:
            days = expected["latest_daily_revenue_cents"] if index == 2 else {
                "2026-01-31": expected["snapshot_revenue_cents"][index],
            }
            return [{"order_day": day, "captured_revenue_cents": 0
                     if failure == "revenue" and index == 1 else value,
                     "order_count": 1, "source_batch_id": batches[index]["batch_id"]}
                    for day, value in days.items()]
        if "dim_customers_scd2" in sql:
            history = expected["customer_history"] if index else [
                {**expected["customer_history"][0], "valid_to": None},
            ]
            return [{**row, "customer_id": "C00000001", "is_current": False
                     if failure == "current_flag" and index == 1 else row["valid_to"] is None,
                     "full_name": "wrong" if failure == "oracle" and index == 1
                     else row["full_name"]} for row in history]
        if "SELECT order_id" in sql:
            return [] if failure == "orders" else expected["latest_orders"]
        return [{"days": 2 if index == 2 else 1, "orders": 2 if index == 2 else 1,
                 "captured_revenue_cents": expected["snapshot_revenue_cents"][index],
                 "invalid_days": 0}]

    report = {}
    if failure:
        with pytest.raises((ValueError, RuntimeError, OSError)):
            exercise_linked_batches(Planner(), query, stage, expected, tmp_path, report)
        assert len(commits) == (3 if failure in {"replay_snapshot", "replay_stage"} else
                                2 if failure == "orders" else 1)
        assert "samples" in report
        attempts = inspect_journal(tmp_path / "run-journal.sqlite3")["runs"]
        assert attempts[0]["outcome"] == "failed"
        assert attempts[0]["checkpoint_completed_events"] == 0
    else:
        exercise_linked_batches(Planner(), query, stage, expected, tmp_path, report)
        assert report["status"] == "ready"
        assert report["idle"]["status"] == "idle"
        assert stages == [*COMMERCE_STAGES, CURRENT_STAGE] * 3 + [CURRENT_STAGE]
        assert [sample["oracle"]["captured_revenue_cents"] for sample in report["samples"]] == (
            [1000, 1500, 2200]
        )
        assert len(commits) == 3
        attempts = inspect_journal(tmp_path / "run-journal.sqlite3")["runs"]
        assert len(attempts) == 4
        assert all(attempt["outcome"] == "ready" for attempt in attempts)
        assert [attempt["checkpoint_completed_events"] for attempt in attempts] == [0, 1, 1, 1]
        assert len(set(run_id for run_id, _ in owners)) == 4
        for i, batch in enumerate(batches):
            selected = owners[i * 9:(i + 1) * 9]
            assert len({run_id for run_id, _ in selected}) == 1
            assert {batch_id for _, batch_id in selected} == {batch["batch_id"]}
        assert owners[-1][0] != owners[-2][0]
        with sqlite3.connect(tmp_path / "run-journal.sqlite3") as connection:
            count = connection.execute(
                "SELECT count(*) FROM commerce_container_events",
            ).fetchone()[0]
            assert count == 28


def test_wrong_pending_scenario_rejected_before_compute(tmp_path: Path):
    class Planner:
        def plan(self, **kwargs):
            return {"batches": []}

    with pytest.raises(ValueError, match="three chronological"):
        exercise_linked_batches(Planner(), lambda _: [], lambda *_: pytest.fail("compute"),
                                {"batches": [{"batch_id": "a" * 16}]}, tmp_path, {})


@pytest.mark.parametrize("value", ["2026-02-01 00:00:00.000", "2026-02-01T00:00:00Z",
                                  "2026-02-01 00:00:00.000000 UTC",
                                  "2026-02-01T03:00:00+03:00"])
def test_timestamp_normalization_preserves_utc_oracle(value: str):
    assert utc_timestamp(value) == "2026-02-01T00:00:00Z"
    assert utc_timestamp(None) is None


def test_main_rejects_unrelated_project_before_artifacts(tmp_path: Path, monkeypatch):
    from lakehouse_ops.commerce_linked_acceptance import main

    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "unrelated")
    monkeypatch.setattr("sys.argv", ["linked", "--output", str(tmp_path / "evidence"),
                                    "--s3-endpoint-url", "http://localhost:19400",
                                    "--server", "http://localhost:18400"])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2
    assert not (tmp_path / "evidence").exists()


def test_timestamp_oracle_does_not_hide_subsecond_changes():
    assert utc_timestamp("2026-02-01 00:00:00.123000 UTC") == "2026-02-01T00:00:00.123000Z"


@pytest.mark.parametrize("failure", [None, "stage", "replay_snapshot", "oracle", "gold",
                                     "partial_checkpoint", "following_history"])
def test_inflight_drill_uses_persisted_retry_before_late_rebuild(tmp_path, failure):
    generate_change_scenario(tmp_path / "scenario")
    expected = json.loads((tmp_path / "scenario" / "expected.json").read_text())
    first, late, newer = expected["batches"]
    following = {"batch_id": "f" * 16, "batch_at": "2026-02-04T00:00:00Z"}
    s3 = FakeS3Client()
    for batch in (first, newer):
        add_snapshot(s3, batch["batch_id"], batch["batch_at"])
    checkpoint = tmp_path / "checkpoint.json"
    factories, stages, modes = [], [], []
    history, snapshots, current, selected = [], 0, None, None
    c_publications = 0

    def planner():
        instance = CommerceBatchPlanner(s3, bucket="lakehouse", state_path=checkpoint)
        factories.append(instance)
        return instance

    def land_late():
        assert len(json.loads(checkpoint.read_text())["processed_batches"]) == 1
        assert stages[-1][1] == "spark-commerce-customer-scd2"
        add_snapshot(s3, late["batch_id"], late["batch_at"])
        if failure == "partial_checkpoint":
            checkpoint.write_text('{}')

    def land_following():
        assert len(json.loads(checkpoint.read_text())["processed_batches"]) == 3
        add_snapshot(s3, following["batch_id"], following["batch_at"])
        return following

    def stage(service, batch, *, journal):
        nonlocal history, snapshots, current, selected, c_publications
        selected = batch
        stages.append((batch["batch_id"], service, journal.run_id))
        for event in ("launch_intent", "completed"):
            journal.record_container(service, f"owned-{len(stages)}", event, batch=batch,
                                     project_hint="lakehouse-ops-linked-unit")
        if service == COMMERCE_STAGES[0]:
            modes.append((batch["batch_id"], batch["customer_history_mode"]))
        if service == "spark-commerce-customer-scd2":
            if batch["batch_id"] == first["batch_id"]:
                history = [{**expected["customer_history"][0], "valid_to": None}]
                snapshots += 1
            elif batch["batch_id"] == newer["batch_id"]:
                c_publications += 1
                if failure == "stage":
                    raise OSError("unexpected publication failure")
                history = [
                    {**expected["customer_history"][0], "valid_to": newer["batch_at"]},
                    {**expected["customer_history"][1], "valid_from": newer["batch_at"]},
                ]
                snapshots += int(c_publications == 1 or failure == "replay_snapshot")
            elif batch["batch_id"] == late["batch_id"]:
                history = expected["customer_history"]
                snapshots += 1
            elif failure == "following_history":
                snapshots += 1
        if service == CURRENT_STAGE and batch["batch_id"] != late["batch_id"]:
            current = batch

    def query(sql):
        if "$snapshots" in sql:
            return [{"snapshots": snapshots}]
        if "SELECT customer_id" in sql:
            return [{**row, "customer_id": "C00000001", "is_current": row["valid_to"] is None}
                    for row in history]
        if "FROM lakehouse.gold.commerce_current_daily" in sql:
            days = ({"2026-01-31": 1000} if current["batch_id"] == first["batch_id"]
                    else expected["latest_daily_revenue_cents"])
            return [{"order_day": day, "captured_revenue_cents":
                     0 if failure == "oracle" and c_publications == 2 else cents,
                     "order_count": 1, "source_batch_id": current["batch_id"]}
                    for day, cents in days.items()]
        if "SELECT order_id" in sql:
            return expected["latest_orders"]
        revenue = (1000 if selected["batch_id"] == first["batch_id"] else
                   1500 if selected["batch_id"] == late["batch_id"] else 2200)
        return [{"days": 0 if failure == "gold" and selected["batch_id"] == late["batch_id"]
                 else 1, "orders": 1, "captured_revenue_cents": revenue, "invalid_days": 0}]

    report = {}
    if failure:
        messages = {"stage": "unexpected publication failure",
                    "replay_snapshot": "C history replay added",
                    "oracle": "current gold differs", "gold": "commerce gold batch is not ready",
                    "partial_checkpoint": "partially published C advanced",
                    "following_history": "unchanged following customer"}
        with pytest.raises((ValueError, RuntimeError, OSError), match=messages[failure]):
            exercise_inflight_arrival(planner, query, stage, expected, tmp_path, report,
                                      land_late, land_following)
        assert report.get("status") != "ready"
        assert report["inflight_recovery"]["status"] != "ready"
        runs = inspect_journal(tmp_path / "run-journal.sqlite3")["runs"]
        assert runs[0]["checkpoint_completed_events"] == 0
        return
    exercise_inflight_arrival(planner, query, stage, expected, tmp_path, report,
                              land_late, land_following)
    sample = report["inflight_recovery"]
    assert report["status"] == sample["status"] == "ready"
    assert sample["checkpoint_before_failure_sha256"] == sample["checkpoint_after_failure_sha256"]
    assert len(factories) == 2 and factories[0] is not factories[1]
    assert modes == [(batch["batch_id"], mode) for batch, mode in (
        (first, "forward"), (newer, "forward"), (newer, "forward"),
        (late, "rebuild"), (following, "forward"),
    )]
    assert sample["idle"]["status"] == "idle"
    assert snapshots == 3
    assert [item["status"] for item in sample["attempts"]] == [
        "ready", "injected_failure", "ready", "ready", "ready",
    ]
    runs = inspect_journal(tmp_path / "run-journal.sqlite3")["runs"]
    assert len(runs) == 5
    assert sum(run["checkpoint_completed_events"] for run in runs) == 4
    failed = [run for run in runs if run["outcome"] == "failed"]
    assert len(failed) == 1 and failed[0]["checkpoint_completed_events"] == 0
    from integration.test_commerce_retained_history_correction import _check_retained_inflight

    retained = tmp_path / "linked.json"
    retained.write_text(json.dumps(report))
    _check_retained_inflight(tmp_path)
    for corruption in ("order", "gold", "history", "checkpoint_hash", "run_id"):
        altered = json.loads(json.dumps(report))
        sample = altered["inflight_recovery"]
        if corruption == "order":
            sample["attempts"][2], sample["attempts"][3] = (
                sample["attempts"][3], sample["attempts"][2],
            )
        elif corruption == "gold":
            sample["attempts"][3]["batch_gold_oracle"]["captured_revenue_cents"] = 2200
        elif corruption == "history":
            sample["published_C_history"][1]["valid_from"] = late["batch_at"]
        elif corruption == "checkpoint_hash":
            sample["checkpoint_sha256"] = "0" * 64
        else:
            sample["attempts"][1]["run_id"] = sample["attempts"][2]["run_id"]
        retained.write_text(json.dumps(altered))
        with pytest.raises(AssertionError):
            _check_retained_inflight(tmp_path)
    retained.write_text(json.dumps(report))


def test_main_rejects_mixed_inflight_and_maintenance_before_artifacts(tmp_path, monkeypatch):
    from lakehouse_ops.commerce_linked_acceptance import main

    monkeypatch.setattr("sys.argv", ["linked", "--output", str(tmp_path / "evidence"),
                                    "--s3-endpoint-url", "http://localhost:19400",
                                    "--server", "http://localhost:18400",
                                    "--late-customer-history", "--inflight-late-arrival"])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2
    assert not (tmp_path / "evidence").exists()
