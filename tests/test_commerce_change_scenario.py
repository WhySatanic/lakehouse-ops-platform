from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakehouse_ops.commerce_change_scenario import generate_change_scenario, main
from lakehouse_ops.ingestion.commerce_bronze import load_commerce_batch


def test_related_snapshots_have_independent_business_expectations(tmp_path: Path) -> None:
    report = generate_change_scenario(tmp_path / "scenario")
    expected = json.loads(Path(report["expected"]).read_text())
    observed_revenue = []
    for batch in report["batches"]:
        root = Path(batch["path"])
        orders = [json.loads(line) for line in (root / "orders.jsonl").read_text().splitlines()]
        observed_revenue.append(sum(order["total_cents"] for order in orders))
        load_commerce_batch(root, expected_batch_id=batch["batch_id"])
    assert observed_revenue == expected["snapshot_revenue_cents"] == [1000, 1500, 2200]
    assert sum(observed_revenue) == 4700  # Historical snapshots cannot be summed as events.
    latest = Path(report["batches"][-1]["path"])
    orders = [json.loads(line) for line in (latest / "orders.jsonl").read_text().splitlines()]
    assert [
        {"order_id": row["order_id"], "total_cents": row["total_cents"]} for row in orders
    ] == expected["latest_orders"]
    assert {row["event_at"][:10]: row["total_cents"] for row in orders} == (
        expected["latest_daily_revenue_cents"]
    )
    assert orders[1]["event_at"] < "2026-01-04"  # More than 30 days before arrival.
    names = [
        json.loads((Path(batch["path"]) / "customers.jsonl").read_text())["full_name"]
        for batch in report["batches"]
    ]
    assert names == ["Alice", "Alice Updated", "Alice Updated"]
    assert len(expected["customer_history"]) == 2


def test_scenario_replay_is_byte_identical(tmp_path: Path) -> None:
    root = tmp_path / "scenario"
    first = generate_change_scenario(root)
    before = {
        path.relative_to(root): path.read_bytes() for path in root.rglob("*") if path.is_file()
    }
    replay = generate_change_scenario(root)
    assert first["created"] is True
    assert replay == {**first, "created": False}
    assert before == {
        path.relative_to(root): path.read_bytes() for path in root.rglob("*") if path.is_file()
    }


@pytest.mark.parametrize("damage", ["orders", "expected", "extra"])
def test_scenario_preserves_conflicting_existing_data(tmp_path: Path, damage: str) -> None:
    root = tmp_path / "scenario"
    report = generate_change_scenario(root)
    target = (
        Path(report["batches"][0]["path"]) / "orders.jsonl"
        if damage == "orders"
        else root / f"{damage}.json"
    )
    target.write_text("changed\n")
    with pytest.raises(ValueError, match="existing scenario differs"):
        generate_change_scenario(root)
    assert target.read_text() == "changed\n"


def test_module_command_reports_success_and_conflict(tmp_path: Path, capsys) -> None:
    root = tmp_path / "scenario"
    assert main(["--output", str(root)]) == 0
    assert len(json.loads(capsys.readouterr().out)["batches"]) == 3
    (root / "expected.json").write_text("changed")
    assert main(["--output", str(root)]) == 2
    assert "existing scenario differs" in capsys.readouterr().err
