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
    assert linked.main() == (2 if failure else 0)
    assert modes == ["rebuild"]
    assert os.environ.get("COMMERCE_CUSTOMER_HISTORY_MODE") == previous
    report = json.loads((tmp_path / "proof/linked.json").read_text())
    assert report["status"] == ("failed" if failure else "ready")
