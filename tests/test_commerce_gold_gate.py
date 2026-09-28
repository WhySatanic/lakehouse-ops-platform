from __future__ import annotations

from typing import Any

import httpx
import pytest

from lakehouse_ops.commerce_gold_gate import (
    CommerceGoldGateError,
    check_commerce_gold,
    check_commerce_gold_with_retry,
)
from lakehouse_ops.trino import TrinoProtocolError, TrinoQueryError


def http_failure(status: int) -> TrinoProtocolError:
    request = httpx.Request("POST", "http://localhost:8080/v1/statement")
    response = httpx.Response(status, request=request)
    error = TrinoProtocolError(f"Trino returned HTTP {status}")
    error.__cause__ = httpx.HTTPStatusError("failure", request=request, response=response)
    return error


@pytest.mark.parametrize("error", [httpx.ConnectError("offline"), http_failure(503)])
def test_gold_gate_retries_transient_failure_then_recovers(error: Exception) -> None:
    calls = 0
    delays: list[float] = []

    def query(sql: str) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise error
        return [{"days": 1, "orders": 2, "captured_revenue_cents": 100, "invalid_days": 0}]

    report = check_commerce_gold_with_retry(
        query, "a" * 16, attempts=3, delay_seconds=2, sleep=delays.append
    )
    assert report.orders == 2
    assert calls == 2
    assert delays == [2]


@pytest.mark.parametrize(
    "error", [TrinoProtocolError("invalid JSON"), TrinoQueryError("invalid SQL"), http_failure(401)]
)
def test_gold_gate_does_not_retry_deterministic_failure(error: Exception) -> None:
    calls = 0
    delays: list[float] = []

    def query(sql: str) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        raise error

    with pytest.raises(type(error)):
        check_commerce_gold_with_retry(query, "a" * 16, attempts=3, sleep=delays.append)
    assert calls == 1
    assert delays == []


def test_gold_gate_retry_is_bounded() -> None:
    calls = 0
    delays: list[float] = []

    def query(sql: str) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("slow coordinator")

    with pytest.raises(httpx.ReadTimeout):
        check_commerce_gold_with_retry(query, "a" * 16, attempts=3, sleep=delays.append)
    assert calls == 3
    assert delays == [2, 2]


def test_gold_gate_does_not_retry_invalid_gold() -> None:
    delays: list[float] = []
    with pytest.raises(CommerceGoldGateError, match="not ready"):
        check_commerce_gold_with_retry(
            lambda sql: [{"days": 0, "orders": 0, "captured_revenue_cents": 0, "invalid_days": 0}],
            "a" * 16, attempts=3, sleep=delays.append,
        )
    assert delays == []


@pytest.mark.parametrize(
    ("attempts", "delay"), [(0, 2), (6, 2), (True, 2), (1, -1), (1, 61), (1, float("nan"))]
)
def test_gold_gate_rejects_invalid_retry_bounds(attempts: int, delay: float) -> None:
    with pytest.raises(CommerceGoldGateError):
        check_commerce_gold_with_retry(
            lambda sql: pytest.fail("query must not run"),
            "a" * 16, attempts=attempts, delay_seconds=delay,
        )


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
