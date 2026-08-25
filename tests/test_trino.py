from __future__ import annotations

import httpx
import pytest

from lakehouse_ops.trino import TrinoClient, TrinoProtocolError, TrinoQueryError


def final_stats(**overrides: int | str) -> dict[str, int | str]:
    stats: dict[str, int | str] = {
        "state": "FINISHED",
        "elapsedTimeMillis": 21,
        "wallTimeMillis": 12,
        "cpuTimeMillis": 7,
        "processedRows": 3,
        "processedBytes": 128,
        "physicalInputBytes": 64,
        "peakMemoryBytes": 256,
        "spilledBytes": 0,
    }
    stats.update(overrides)
    return stats


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
                "stats": final_stats(),
            },
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = TrinoClient("http://localhost:8080", client=http_client)

    assert client.query("SELECT 3") == [{"count": 3}]
    assert requests[0].headers["x-trino-user"] == "lakehouse-ops"
    assert str(requests[1].url) == "http://localhost:8080/v1/statement/query-1/1"


def test_query_with_stats_returns_final_execution_metrics() -> None:
    transport = httpx.MockTransport(
        lambda _: httpx.Response(
            200,
            json={
                "id": "query-stats",
                "columns": [{"name": "result", "type": "varchar"}],
                "data": [["plan"]],
                "stats": final_stats(),
            },
        )
    )
    client = TrinoClient("http://localhost:8080", client=httpx.Client(transport=transport))

    result = client.query_with_stats("EXPLAIN ANALYZE SELECT 1")

    assert result.query_id == "query-stats"
    assert result.rows == ({"result": "plan"},)
    assert result.stats.wall_time_ms == 12
    assert result.stats.cpu_time_ms == 7
    assert result.stats.processed_bytes == 128
    assert result.stats.peak_memory_bytes == 256


def test_query_with_stats_rejects_incomplete_final_statistics() -> None:
    transport = httpx.MockTransport(
        lambda _: httpx.Response(
            200,
            json={"id": "query-stats", "stats": final_stats(state="RUNNING")},
        )
    )
    client = TrinoClient("http://localhost:8080", client=httpx.Client(transport=transport))

    with pytest.raises(TrinoProtocolError, match="not FINISHED"):
        client.query_with_stats("SELECT 1")


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
