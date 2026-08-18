from __future__ import annotations

import httpx
import pytest

from lakehouse_ops.trino import TrinoClient, TrinoProtocolError, TrinoQueryError


def test_query_collects_paginated_rows_and_rewrites_internal_next_uri() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(
                200,
                json={
                    "id": "query-1",
                    "nextUri": "http://trino-coordinator:8080/v1/statement/query-1/1",
                },
            )
        return httpx.Response(
            200,
            json={
                "id": "query-1",
                "columns": [{"name": "count", "type": "bigint"}],
                "data": [[3]],
            },
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = TrinoClient("http://localhost:8080", client=http_client)

    assert client.query("SELECT 3") == [{"count": 3}]
    assert requests[0].headers["x-trino-user"] == "lakehouse-ops"
    assert str(requests[1].url) == "http://localhost:8080/v1/statement/query-1/1"


def test_query_surfaces_trino_error() -> None:
    transport = httpx.MockTransport(
        lambda _: httpx.Response(
            200,
            json={
                "id": "query-2",
                "error": {"errorName": "TABLE_NOT_FOUND", "message": "missing table"},
            },
        )
    )
    client = TrinoClient("http://localhost:8080", client=httpx.Client(transport=transport))

    with pytest.raises(TrinoQueryError, match="TABLE_NOT_FOUND: missing table"):
        client.query("SELECT * FROM missing")


def test_query_rejects_unexpected_next_uri_path() -> None:
    transport = httpx.MockTransport(
        lambda _: httpx.Response(
            200,
            json={
                "id": "query-3",
                "nextUri": "https://example.test/unexpected",
            },
        )
    )
    client = TrinoClient("http://localhost:8080", client=httpx.Client(transport=transport))

    with pytest.raises(TrinoProtocolError, match="unexpected path"):
        client.query("SELECT 1")
