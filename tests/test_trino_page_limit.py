from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from test_commerce_pipeline import Planner
from test_trino import final_stats

from lakehouse_ops import commerce_pipeline as pipeline
from lakehouse_ops.trino import TrinoClient, TrinoProtocolError


def paginated_query(
    terminal_page: int | None, requests: list[httpx.Request], cancel: int | str = 204,
) -> Callable[[httpx.Request], httpx.Response]:
    page = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal page
        requests.append(request)
        if request.method == "DELETE":
            assert str(request.url) == (
                "http://localhost:8080/v1/statement/owned/1000?token=latest"
            )
            assert request.headers["X-Trino-User"] == "reader"
            assert request.extensions["timeout"]["read"] == 5.0
            if cancel == "transport":
                raise httpx.ConnectError("offline")
            if cancel == "timeout":
                raise httpx.ReadTimeout("cancellation expired")
            return httpx.Response(cancel)
        page += 1
        payload = {
            "id": "owned", "columns": [{"name": "page"}], "data": [[page]],
            "stats": final_stats(),
        }
        if page != terminal_page:
            payload["nextUri"] = (
                f"http://internal:8080/v1/statement/owned/{page}?token=latest"
            )
        return httpx.Response(200, json=payload)

    return handler


@pytest.mark.parametrize("pages", [999, 1000])
def test_terminal_response_within_page_limit_returns_all_rows(pages: int) -> None:
    requests: list[httpx.Request] = []
    with httpx.Client(transport=httpx.MockTransport(paginated_query(pages, requests))) as http:
        result = TrinoClient("http://localhost:8080", client=http).query("SELECT page")
    assert result == [{"page": page} for page in range(1, pages + 1)]
    assert len(requests) == pages
    assert all(request.method != "DELETE" for request in requests)


@pytest.mark.parametrize("terminal_page", [1001, None])
@pytest.mark.parametrize("cancel", [204, 403, "transport", "timeout"])
@pytest.mark.parametrize("budget", [None, 60.0])
def test_page_limit_stops_before_extra_get_and_cancels_latest_uri(
    terminal_page: int | None, cancel: int | str, budget: float | None,
) -> None:
    requests: list[httpx.Request] = []
    handler = paginated_query(terminal_page, requests, cancel)
    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as http,
        pytest.raises(TrinoProtocolError, match="exceeded the 1000-page safety limit") as caught,
    ):
        TrinoClient("http://localhost:8080", user="reader", client=http,
                    query_timeout=budget).query("SELECT page")
    assert sum(request.method != "DELETE" for request in requests) == 1000
    assert requests[-1].method == "DELETE"
    assert getattr(caught.value, "query_id", None) == "owned"
    assert getattr(caught.value, "cancellation", None) == {
        204: "accepted", 403: "http_403",
        "transport": "transport_failed", "timeout": "transport_failed",
    }[cancel]


def test_page_limit_in_gold_verification_prevents_checkpoint() -> None:
    planner = Planner()
    requests: list[httpx.Request] = []
    handler = paginated_query(None, requests)
    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = TrinoClient("http://localhost:8080", client=http, user="reader")
        with pytest.raises(pipeline.CommercePipelineError, match="verify-commerce-gold") as caught:
            pipeline.run_commerce_batch(
                planner, client.query, run_stage=lambda service, batch: None,
            )
    assert isinstance(caught.value.__cause__, TrinoProtocolError)
    assert planner.commits == []
    assert requests[-1].method == "DELETE"
