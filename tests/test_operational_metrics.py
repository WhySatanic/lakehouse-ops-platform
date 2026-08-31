from __future__ import annotations

import io
import json
import threading
from http.server import ThreadingHTTPServer
from typing import Any
from urllib.request import Request, urlopen

import pytest

import lakehouse_ops.operational_metrics as operational_metrics
from lakehouse_ops.operational_metrics import (
    OPERATIONAL_SQL,
    OperationalMetricsError,
    OperationalSnapshot,
    fetch_operational_snapshot,
    make_handler,
    parse_operational_snapshot,
    render_failure,
    render_prometheus,
)


def pages(row: list[Any]) -> list[dict[str, Any]]:
    return [
        {"nextUri": "http://trino/next"},
        {
            "columns": [
                {"name": "row_count"},
                {"name": "latest_ingested_at"},
                {"name": "data_file_count"},
                {"name": "small_file_count"},
                {"name": "snapshot_count"},
            ],
            "data": [row],
        },
    ]


def test_parse_operational_snapshot_accepts_one_complete_row() -> None:
    assert parse_operational_snapshot(pages([2, 1_786_000_000.5, 3, 2, 4])) == (
        OperationalSnapshot(
            row_count=2,
            latest_ingested_at=1_786_000_000.5,
            data_file_count=3,
            small_file_count=2,
            snapshot_count=4,
        )
    )


def test_fetch_operational_snapshot_follows_trino_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response_pages = iter(
        [
            {"nextUri": "http://trino/next"},
            pages([2, 1_786_000_000.5, 3, 2, 4])[1],
        ]
    )
    requests: list[Request] = []

    def fake_urlopen(request: Request, timeout: float) -> io.BytesIO:
        requests.append(request)
        assert timeout == 3.0
        return io.BytesIO(json.dumps(next(response_pages)).encode())

    monkeypatch.setattr(operational_metrics, "urlopen", fake_urlopen)

    snapshot = fetch_operational_snapshot("http://trino/", "lakehouse-ops", 3.0)

    assert snapshot == OperationalSnapshot(2, 1_786_000_000.5, 3, 2, 4)
    assert requests[0].full_url == "http://trino/v1/statement"
    assert requests[0].data == OPERATIONAL_SQL.encode()
    assert requests[0].get_header("X-trino-user") == "lakehouse-ops"
    assert requests[1].full_url == "http://trino/next"


def test_parse_operational_snapshot_surfaces_trino_error() -> None:
    with pytest.raises(OperationalMetricsError, match="TABLE_NOT_FOUND"):
        parse_operational_snapshot(
            [{"error": {"errorName": "TABLE_NOT_FOUND", "message": "missing"}}]
        )


@pytest.mark.parametrize(
    "row, message",
    [
        ([0, 1_786_000_000.0, 3, 2, 4], "row_count"),
        ([2, None, 3, 2, 4], "latest_ingested_at"),
        ([2, 1_786_000_000.0, 0, 0, 4], "data_file_count"),
        ([2, 1_786_000_000.0, 3, -1, 4], "small_file_count"),
        ([2, 1_786_000_000.0, 3, 4, 4], "exceeds"),
        ([2, 1_786_000_000.0, 3, 2, 0], "snapshot_count"),
    ],
)
def test_parse_operational_snapshot_rejects_invalid_values(
    row: list[Any], message: str
) -> None:
    with pytest.raises(OperationalMetricsError, match=message):
        parse_operational_snapshot(pages(row))


def test_parse_operational_snapshot_rejects_unexpected_shape() -> None:
    with pytest.raises(OperationalMetricsError, match="unexpected"):
        parse_operational_snapshot([{"columns": [{"name": "row_count"}], "data": []}])


def test_render_prometheus_exposes_freshness_and_maintenance_gauges() -> None:
    body = render_prometheus(
        OperationalSnapshot(
            row_count=2,
            latest_ingested_at=1_000.0,
            data_file_count=3,
            small_file_count=2,
            snapshot_count=4,
        ),
        1_100.0,
    )

    assert "lakehouse_operational_collector_success 1" in body
    assert (
        'lakehouse_ingestion_freshness_age_seconds{table="lakehouse.silver.weather_hourly"} '
        "100.000" in body
    )
    assert 'lakehouse_maintenance_data_files{table="lakehouse.silver.weather_hourly"} 3' in body
    assert (
        'lakehouse_maintenance_small_file_backlog{table="lakehouse.silver.weather_hourly"} 2'
        in body
    )
    assert body.endswith("\n")


def test_render_prometheus_clamps_future_ingestion_age_to_zero() -> None:
    body = render_prometheus(
        OperationalSnapshot(2, 2_000.0, 3, 2, 4),
        1_000.0,
    )

    assert "lakehouse_ingestion_freshness_age_seconds" in body
    assert " 0.000\n" in body


def test_render_prometheus_rejects_invalid_collection_time() -> None:
    with pytest.raises(OperationalMetricsError, match="collection timestamp"):
        render_prometheus(OperationalSnapshot(2, 1_000.0, 3, 2, 4), float("nan"))


def test_render_failure_marks_collector_unhealthy() -> None:
    assert render_failure().endswith("lakehouse_operational_collector_success 0\n")


def test_metrics_handler_serves_health_and_live_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        operational_metrics,
        "fetch_operational_snapshot",
        lambda server, user, timeout: OperationalSnapshot(2, 1_000.0, 3, 2, 4),
    )
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), make_handler("http://trino", "lakehouse-ops", 3.0)
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        with urlopen(f"{base_url}/-/healthy", timeout=2) as response:
            assert response.read() == b"ok\n"
        with urlopen(f"{base_url}/metrics", timeout=2) as response:
            body = response.read().decode()
            assert response.headers["Content-Type"].startswith("text/plain")
            assert "lakehouse_operational_collector_success 1" in body
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
