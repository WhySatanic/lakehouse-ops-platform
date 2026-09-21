from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakehouse_ops.ingestion.commerce_fixture import (
    CommerceFixtureConfig,
    CommerceFixtureError,
    generate_commerce_fixture,
)


def _read_json_lines(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_fixture_is_relational_and_contains_exact_quality_cases(tmp_path: Path) -> None:
    config = CommerceFixtureConfig(
        customers=8,
        products=4,
        orders=12,
        null_customer_emails=2,
        duplicate_orders=3,
        late_orders=4,
        invalid_payments=2,
        seed=7,
    )

    result = generate_commerce_fixture(tmp_path, config)
    customers = _read_json_lines(result.path / "customers.jsonl")
    products = _read_json_lines(result.path / "products.jsonl")
    orders = _read_json_lines(result.path / "orders.jsonl")
    payments = _read_json_lines(result.path / "payments.jsonl")

    assert result.created is True
    assert result.tables == {"customers": 8, "products": 4, "orders": 15, "payments": 12}
    assert sum(customer["email"] is None for customer in customers) == 2
    assert len(orders) - len({order["order_id"] for order in orders}) == 3
    assert sum(order["event_at"] < "2026-01-01" for order in orders[:12]) == 4
    assert sum(order["event_at"] < "2026-01-01" for order in orders) == 4
    assert sum(payment["amount_cents"] is None for payment in payments) == 2
    assert {order["customer_id"] for order in orders} <= {
        customer["customer_id"] for customer in customers
    }
    assert {order["product_id"] for order in orders} <= {
        product["product_id"] for product in products
    }
    assert {payment["order_id"] for payment in payments} == {
        order["order_id"] for order in orders
    }


def test_fixture_rerun_is_idempotent_and_detects_tampering(tmp_path: Path) -> None:
    config = CommerceFixtureConfig(
        customers=2,
        products=2,
        orders=3,
        null_customer_emails=1,
        duplicate_orders=1,
        late_orders=1,
        invalid_payments=1,
        seed=11,
    )
    first = generate_commerce_fixture(tmp_path, config)
    second = generate_commerce_fixture(tmp_path, config)

    assert second.batch_id == first.batch_id
    assert second.created is False

    (first.path / "orders.jsonl").write_text("tampered\n", encoding="utf-8")
    with pytest.raises(CommerceFixtureError, match="checksum verification"):
        generate_commerce_fixture(tmp_path, config)


def test_same_configuration_has_identical_content_in_different_roots(tmp_path: Path) -> None:
    config = CommerceFixtureConfig(
        customers=3,
        products=2,
        orders=5,
        null_customer_emails=1,
        duplicate_orders=1,
        late_orders=1,
        invalid_payments=1,
        seed=19,
    )
    first = generate_commerce_fixture(tmp_path / "one", config)
    second = generate_commerce_fixture(tmp_path / "two", config)

    assert first.batch_id == second.batch_id
    for filename in ("customers.jsonl", "products.jsonl", "orders.jsonl", "payments.jsonl"):
        assert (first.path / filename).read_bytes() == (second.path / filename).read_bytes()


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"orders": 0}, "must be positive"),
        ({"customers": 2, "null_customer_emails": 3}, "must be between"),
        ({"orders": 2, "duplicate_orders": 3}, "must be between"),
        (
            {
                "orders": 2,
                "late_orders": 1,
                "duplicate_orders": 2,
                "invalid_payments": 0,
            },
            "must fit in distinct",
        ),
        ({"batch_at": "2026-01-31"}, "must include a timezone"),
    ],
)
def test_fixture_rejects_invalid_configuration(
    overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(CommerceFixtureError, match=message):
        CommerceFixtureConfig(**overrides)
