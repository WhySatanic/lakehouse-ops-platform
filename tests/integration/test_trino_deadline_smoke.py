"""Opt-in cancellation against a caller-selected, local OSS Trino coordinator."""

from __future__ import annotations

import os
import time

import httpx
import pytest

from lakehouse_ops.trino import TrinoClient, TrinoQueryTimeoutError


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_TRINO_DEADLINE_SMOKE") != "1", reason="requires local Trino",
)
def test_real_query_cancellation_after_delayed_initial_response() -> None:
    server = os.environ["LAKEOPS_TRINO_DEADLINE_SERVER"]
    requests = []

    def delay_initial_response(response: httpx.Response) -> None:
        requests.append(response.request.method)
        if response.request.method == "POST":
            # Delay only this client's response processing, not the shared coordinator.
            time.sleep(5.1)

    with httpx.Client(event_hooks={"response": [delay_initial_response]}) as http:
        client = TrinoClient(server, user="lakehouse-performance", client=http, query_timeout=5)
        with pytest.raises(TrinoQueryTimeoutError) as caught:
            client.query("SELECT 1 AS value")
    assert caught.value.query_id
    assert caught.value.cancellation == "accepted"
    assert requests == ["POST", "DELETE"]
    with TrinoClient(server, user="lakehouse-performance", query_timeout=15) as healthy:
        assert healthy.query("SELECT 1 AS value") == [{"value": 1}]
