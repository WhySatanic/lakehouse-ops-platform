from __future__ import annotations

import httpx
import pytest
from test_commerce_pipeline import Planner
from test_trino import final_stats

from lakehouse_ops import commerce_pipeline as pipeline
from lakehouse_ops import trino
from lakehouse_ops.trino import TrinoClient, TrinoQueryTimeoutError


@pytest.mark.parametrize("budget", [0, -1, float("inf"), float("nan"), True])
def test_invalid_query_budget(budget: float) -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        TrinoClient("http://localhost:8080", query_timeout=budget)


@pytest.mark.parametrize("cancel", [204, 403, "transport"])
def test_deadline_cancels_latest_owned_uri_and_discards_partial_rows(
    monkeypatch: pytest.MonkeyPatch, cancel: int | str,
) -> None:
    clock = [0.0]
    requests = []
    monkeypatch.setattr(trino, "monotonic", lambda: clock[0])

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "DELETE":
            assert str(request.url) == "http://localhost:8080/v1/statement/owned/2?token=abc"
            assert request.headers["X-Trino-User"] == "reader"
            assert request.extensions["timeout"]["read"] == 5.0
            if cancel == "transport":
                raise httpx.ConnectError("offline")
            return httpx.Response(cancel)
        if request.method == "POST":
            assert request.extensions["timeout"]["read"] == 1.0
            clock[0] = 0.4
            return httpx.Response(200, json={
                "id": "owned", "nextUri": "http://internal:8080/v1/statement/owned/1",
            })
        assert request.extensions["timeout"]["read"] == pytest.approx(0.6)
        clock[0] = 1.1
        return httpx.Response(200, json={
            "id": "owned", "columns": [{"name": "partial"}], "data": [[1]],
            "nextUri": "http://internal:8080/v1/statement/owned/2?token=abc",
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = TrinoClient("http://localhost:8080", user="reader", client=http,
                             query_timeout=1.0)
        with pytest.raises(TrinoQueryTimeoutError) as caught:
            client.query("SELECT 1")
    assert caught.value.query_id == "owned"
    assert caught.value.cancellation == {
        204: "accepted", 403: "http_403", "transport": "transport_failed",
    }[cancel]
    assert [request.method for request in requests] == ["POST", "GET", "DELETE"]


@pytest.mark.parametrize("initial", [True, False])
def test_http_timeout_retains_query_identity_when_available(initial: bool) -> None:
    methods = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        if request.method == "DELETE":
            return httpx.Response(204)
        if request.method == "POST" and not initial:
            return httpx.Response(200, json={
                "id": "owned", "nextUri": "http://internal/v1/statement/owned/1",
            })
        raise httpx.ReadTimeout("phase expired")

    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as http,
        pytest.raises(TrinoQueryTimeoutError) as caught,
    ):
        TrinoClient("http://localhost:8080", client=http, query_timeout=1).query("SELECT 1")
    assert caught.value.query_id == (None if initial else "owned")
    assert caught.value.cancellation == ("not_available" if initial else "accepted")
    assert methods == (["POST"] if initial else ["POST", "GET", "DELETE"])


def test_late_terminal_result_is_not_returned_or_cancelled(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [0.0]
    monkeypatch.setattr(trino, "monotonic", lambda: clock[0])

    def handler(_: httpx.Request) -> httpx.Response:
        clock[0] = 2.0
        return httpx.Response(200, json={"id": "done", "stats": final_stats()})

    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as http,
        pytest.raises(TrinoQueryTimeoutError) as caught,
    ):
        TrinoClient("http://localhost:8080", client=http, query_timeout=1).query("SELECT 1")
    assert caught.value.cancellation == "not_available"


def test_budget_resets_between_queries_and_preserves_shorter_phase_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    monkeypatch.setattr(trino, "monotonic", lambda: clock[0])

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.extensions["timeout"]["read"] == 0.25
        assert 0 < request.extensions["timeout"]["connect"] <= 1
        clock[0] += 0.6
        return httpx.Response(200, json={"id": "done", "stats": final_stats()})

    with httpx.Client(transport=httpx.MockTransport(handler),
                      timeout=httpx.Timeout(None, read=0.25)) as http:
        client = TrinoClient("http://localhost:8080", client=http, query_timeout=1)
        assert client.query("SELECT 1") == []
        assert client.query("SELECT 1") == []


def test_default_budget_keeps_http_timeout_behavior() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("unchanged")

    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as http,
        pytest.raises(httpx.ReadTimeout, match="unchanged"),
    ):
        TrinoClient("http://localhost:8080", client=http).query("SELECT 1")


def test_expired_budget_prevents_initial_request(monkeypatch: pytest.MonkeyPatch) -> None:
    ticks = iter([0.0, 2.0])
    monkeypatch.setattr(trino, "monotonic", lambda: next(ticks))

    def forbidden(_: httpx.Request) -> httpx.Response:
        pytest.fail("must not launch a query after the budget expires")

    with (
        httpx.Client(transport=httpx.MockTransport(forbidden)) as http,
        pytest.raises(TrinoQueryTimeoutError, match="not_available"),
    ):
        TrinoClient("http://localhost:8080", client=http, query_timeout=1).query("SELECT 1")


def test_gold_query_timeout_prevents_checkpoint() -> None:
    planner = Planner()
    stages = []

    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("gold query phase expired")

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = TrinoClient("http://localhost:8080", client=http, query_timeout=1)
        with pytest.raises(pipeline.CommercePipelineError, match="verify-commerce-gold") as caught:
            pipeline.run_commerce_batch(
                planner, client.query, run_stage=lambda service, batch: stages.append(service),
            )
    assert isinstance(caught.value.__cause__, TrinoQueryTimeoutError)
    assert stages == list(pipeline.COMMERCE_STAGES)
    assert planner.commits == []
