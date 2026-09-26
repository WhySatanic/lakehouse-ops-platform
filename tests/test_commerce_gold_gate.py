from __future__ import annotations

from typing import Any

import pytest

from lakehouse_ops.commerce_gold_gate import CommerceGoldGateError, check_commerce_gold


def test_checks_selected_batch_with_trino() -> None:
    statements: list[str] = []

    def query(sql: str) -> list[dict[str, Any]]:
        statements.append(sql)
        return [
            {"days": 6, "orders": 60, "captured_revenue_cents": 11_473, "invalid_days": 0}
        ]

    report = check_commerce_gold(query, "a" * 16)

    assert report.as_dict() == {
        "status": "ready",
        "table": "lakehouse.gold.commerce_daily",
        "batch_id": "a" * 16,
        "days": 6,
        "orders": 60,
        "captured_revenue_cents": 11_473,
    }
    assert "source_batch_id = 'aaaaaaaaaaaaaaaa'" in statements[0]
    assert "invalid_days" in statements[0]


@pytest.mark.parametrize("batch_id", ["abc", "A" * 16, "a' OR 1=1 --"])
def test_rejects_invalid_batch_id_before_query(batch_id: str) -> None:
    with pytest.raises(CommerceGoldGateError, match="batch ID"):
        check_commerce_gold(lambda sql: pytest.fail("query must not run"), batch_id)


@pytest.mark.parametrize(
    ("row", "message"),
    [
        ({"days": 0, "orders": 0, "captured_revenue_cents": 0, "invalid_days": 0}, "not ready"),
        ({"days": 1, "orders": 2, "captured_revenue_cents": 0, "invalid_days": 1}, "not ready"),
        (
            {"days": True, "orders": 2, "captured_revenue_cents": 0, "invalid_days": 0},
            "days is invalid",
        ),
        (
            {"days": 1, "orders": 2, "captured_revenue_cents": -1, "invalid_days": 0},
            "captured_revenue_cents is invalid",
        ),
    ],
)
def test_rejects_missing_or_invalid_gold_rows(row: dict[str, Any], message: str) -> None:
    with pytest.raises(CommerceGoldGateError, match=message):
        check_commerce_gold(lambda sql: [row], "a" * 16)


def test_rejects_unexpected_trino_result_shape() -> None:
    with pytest.raises(CommerceGoldGateError, match="unexpected"):
        check_commerce_gold(lambda sql: [], "a" * 16)
