from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from lakehouse_ops.commerce_change_scenario import generate_change_scenario, main
from lakehouse_ops.commerce_linked_acceptance import CURRENT_STAGE, exercise_linked_batches
from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES
from lakehouse_ops.ingestion.commerce_bronze import load_commerce_batch


def test_refund_snapshots_are_complete_and_have_literal_expectations(tmp_path: Path):
    report = generate_change_scenario(tmp_path / "refund", scenario="refund")
    expected = json.loads(Path(report["expected"]).read_text())
    assert [b["label"] for b in report["batches"]] == [
        "initial", "correction", "refund", "late-arrival", "following",
    ]
    assert expected["snapshot_revenue_cents"] == [1000, 1500, 0, 700, 1000]
    assert expected["snapshot_order_counts"] == [1, 1, 1, 2, 3]
    for index, batch in enumerate(report["batches"]):
        root = Path(batch["path"])
        load_commerce_batch(root, expected_batch_id=batch["batch_id"])
        payments = [json.loads(line) for line in (root / "payments.jsonl").read_text().splitlines()]
        assert len(payments) == [1, 1, 1, 2, 3][index]
        orders = [json.loads(line) for line in (root / "orders.jsonl").read_text().splitlines()]
        assert all(order["event_at"] <= order["ingested_at"] == batch["batch_at"]
                   for order in orders)
        assert payments[0]["payment_id"] == "PAY0000000001"
        assert payments[0]["status"] == ("captured" if index < 2 else "refunded")
        assert payments[0]["amount_cents"] == (1000 if index == 0 else 1500)
        assert sum(p["amount_cents"] for p in payments if p["status"] == "captured") == (
            expected["snapshot_revenue_cents"][index]
        )
    assert expected["latest_daily_revenue_cents"] == {
        "2026-01-01": 700, "2026-01-31": 0, "2026-02-05": 300,
    }


def test_refund_source_replay_preserves_bytes_and_rejects_correction_mix(tmp_path: Path):
    root = tmp_path / "scenario"
    first = generate_change_scenario(root, scenario="refund")
    original = {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    assert generate_change_scenario(root, scenario="refund") == {**first, "created": False}
    with pytest.raises(ValueError, match="existing scenario differs"):
        generate_change_scenario(root)
    assert original == {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_refund_cli_selects_the_same_complete_fixture(tmp_path: Path, capsys):
    assert main(["--output", str(tmp_path / "refund"), "--scenario", "refund"]) == 0
    assert len(json.loads(capsys.readouterr().out)["batches"]) == 5


def test_unknown_scenario_does_not_publish(tmp_path: Path):
    with pytest.raises(ValueError, match="unsupported"):
        generate_change_scenario(tmp_path / "absent", scenario="unrelated")
    assert not (tmp_path / "absent").exists()


def test_default_fixture_stays_byte_identical(tmp_path: Path):
    root = tmp_path / "scenario"
    generate_change_scenario(root)
    files = {p.relative_to(root).as_posix(): p.read_bytes()
             for p in root.rglob("*") if p.is_file()}
    digest = hashlib.sha256(b"".join(n.encode() + b"\0" + files[n] for n in sorted(files)))
    assert digest.hexdigest() == "549fc059cadca2260f22cfed76757093cdb14b1b275237aec75cf61f62ef67a5"


@pytest.mark.parametrize("damage", [None, "current", "historical", "payment"])
def test_refund_oracles_run_before_checkpoint_and_replay_before_following(tmp_path, damage):
    generate_change_scenario(tmp_path / "fixture", scenario="refund")
    expected = json.loads((tmp_path / "fixture/expected.json").read_text())
    batches = [{**b, "manifest_sha256": "a" * 64} for b in expected["batches"]]
    committed, stages = [], []
    index = 0

    class Planner:
        def plan(self, **kwargs):
            return {"batches": batches[len(committed):][:kwargs["max_batches"]]}

        def execution_options(self, selected):
            return {"customer_history_mode": "forward"}

        def commit(self, batch_id, **kwargs):
            assert report["samples"][-1]["oracle"]["batch_id"] == batch_id
            committed.append(batch_id)
            return {"created": True}

    def stage(service, batch, *, journal):
        nonlocal index
        index = next(i for i, b in enumerate(batches) if b["batch_id"] == batch["batch_id"])
        stages.append(service)

    def query(sql):
        if "$snapshots" in sql:
            return [{"snapshots": index + 1}]
        if "commerce_current_daily" in sql:
            return [{"source_batch_id": batches[index]["batch_id"], "order_day": day,
                     "captured_revenue_cents": 1500 if damage == "current" and index == 2 else n,
                     "order_count": 1}
                    for day, n in expected["snapshot_daily_revenue_cents"][index].items()]
        if "dim_customers_scd2" in sql:
            history = expected["customer_history"] if index else [
                {**expected["customer_history"][0], "valid_to": None},
            ]
            return [{**row, "customer_id": "C00000001", "is_current": row["valid_to"] is None}
                    for row in history]
        if "SELECT order_id" in sql:
            return expected["latest_orders"]
        if "SELECT source_batch_id" in sql and "commerce_daily" in sql:
            rows = [{"source_batch_id": b["batch_id"], "order_day": day,
                     "captured_revenue_cents": n, "order_count": 1}
                    for i, b in enumerate(batches[:index + 1])
                    for day, n in expected["snapshot_daily_revenue_cents"][i].items()]
            if damage == "historical" and index == 2:
                rows[0]["captured_revenue_cents"] = 0
            return sorted(rows, key=lambda r: (r["source_batch_id"], r["order_day"]))
        if "FROM lakehouse.silver.commerce_payments" in sql:
            rows = [{"source_batch_id": b["batch_id"], "payment_id": p,
                     "status": state, "amount_cents": amount}
                    for i, b in enumerate(batches[:index + 1])
                    for p, state, amount in expected["snapshot_payment_states"][i]]
            if damage == "payment" and index == 2:
                rows[-1]["status"] = "captured"
            return sorted(rows, key=lambda r: (r["source_batch_id"], r["payment_id"]))
        return [{"days": len(expected["snapshot_daily_revenue_cents"][index]),
                 "orders": expected["snapshot_order_counts"][index], "invalid_days": 0,
                 "captured_revenue_cents": expected["snapshot_revenue_cents"][index]}]

    report = {}
    if damage:
        with pytest.raises(ValueError, match="differs"):
            exercise_linked_batches(Planner(), query, stage, expected, tmp_path, report)
        assert committed == [b["batch_id"] for b in batches[:2]]
    else:
        exercise_linked_batches(Planner(), query, stage, expected, tmp_path, report)
        assert committed == [b["batch_id"] for b in batches]
        assert report["idle"]["status"] == "idle"
        assert report["refund_replay"]["status"] == "ready"
        assert report["refund_replay"]["oracle"]["captured_revenue_cents"] == 0
        assert stages == ([*COMMERCE_STAGES, CURRENT_STAGE] * 3 + [CURRENT_STAGE]
                          + [*COMMERCE_STAGES, CURRENT_STAGE] * 2 + [CURRENT_STAGE])
