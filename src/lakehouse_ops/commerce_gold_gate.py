from __future__ import annotations

import logging
import math
import re
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any

import httpx

from lakehouse_ops.trino import TrinoProtocolError

TABLE = "lakehouse.gold.commerce_daily"


class CommerceGoldGateError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CommerceGoldReport:
    batch_id: str
    days: int
    orders: int
    captured_revenue_cents: int

    def as_dict(self) -> dict[str, Any]:
        return {"status": "ready", "table": TABLE, **asdict(self)}


def check_commerce_gold_with_retry(
    query: Callable[[str], list[dict[str, Any]]],
    batch_id: str,
    *,
    attempts: int = 1,
    delay_seconds: float = 2,
    sleep: Callable[[float], None] = time.sleep,
) -> CommerceGoldReport:
    if isinstance(attempts, bool) or not isinstance(attempts, int) or not 1 <= attempts <= 5:
        raise CommerceGoldGateError("attempts must be an integer between 1 and 5")
    if (
        isinstance(delay_seconds, bool)
        or not isinstance(delay_seconds, (int, float))
        or not math.isfinite(delay_seconds)
        or not 0 <= delay_seconds <= 60
    ):
        raise CommerceGoldGateError("delay_seconds must be finite and between 0 and 60")
    for attempt in range(1, attempts + 1):
        try:
            return check_commerce_gold(query, batch_id)
        except (httpx.TransportError, TrinoProtocolError) as error:
            retryable = isinstance(error, httpx.TransportError) or (
                isinstance(error.__cause__, httpx.HTTPStatusError)
                and error.__cause__.response.status_code in {429, 502, 503, 504}
            )
            if not retryable or attempt == attempts:
                raise
            logging.getLogger(__name__).warning(
                "Retrying commerce gold query (%s/%s)", attempt + 1, attempts
            )
            sleep(delay_seconds)
    raise AssertionError("commerce gold retry loop exhausted unexpectedly")


def check_commerce_gold(
    query: Callable[[str], list[dict[str, Any]]], batch_id: str
) -> CommerceGoldReport:
    if not re.fullmatch(r"[0-9a-f]{16}", batch_id):
        raise CommerceGoldGateError("batch ID must be 16 lowercase hexadecimal characters")

    rows = query(
        "SELECT count(*) AS days, "
        "coalesce(sum(order_count), 0) AS orders, "
        "coalesce(sum(captured_revenue_cents), 0) AS captured_revenue_cents, "
        "count_if(order_count IS NULL OR order_count <= 0 OR "
        "captured_revenue_cents IS NULL OR captured_revenue_cents < 0) AS invalid_days "
        f"FROM {TABLE} WHERE source_batch_id = '{batch_id}'"
    )
    if len(rows) != 1 or not isinstance(rows[0], dict):
        raise CommerceGoldGateError("Trino returned an unexpected commerce gold result")
    row = rows[0]
    days = _non_negative_int(row.get("days"), "days")
    orders = _non_negative_int(row.get("orders"), "orders")
    revenue = _non_negative_int(row.get("captured_revenue_cents"), "captured_revenue_cents")
    invalid_days = _non_negative_int(row.get("invalid_days"), "invalid_days")
    if days == 0 or orders == 0 or invalid_days:
        raise CommerceGoldGateError(
            f"commerce gold batch is not ready: days={days}, orders={orders}, "
            f"invalid_days={invalid_days}"
        )
    return CommerceGoldReport(batch_id, days, orders, revenue)


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CommerceGoldGateError(f"Trino commerce gold field {name} is invalid")
    return value
