from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
from test_commerce_pipeline import Planner

from lakehouse_ops import commerce_pipeline as pipeline
from lakehouse_ops.trino import TrinoClient, TrinoQueryTimeoutError


@pytest.fixture
def slow_trino() -> Iterator[tuple[str, list[str], threading.Event]]:
    methods: list[str] = []
    disconnected = threading.Event()
    slow_cancel = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_: object) -> None:
            pass

        def do_POST(self) -> None:
            methods.append("POST")
            sql = self.rfile.read(int(self.headers["Content-Length"]))
            if sql in {b"paged", b"slow-cancel"}:
                if sql == b"slow-cancel":
                    slow_cancel.set()
                self.respond(json.dumps({
                    "id": "owned", "nextUri": f"{address}/v1/statement/owned/1",
                }).encode())
            else:
                self.trickle()

        def do_GET(self) -> None:
            methods.append("GET")
            self.trickle()

        def do_DELETE(self) -> None:
            methods.append("DELETE")
            if slow_cancel.is_set():
                self.trickle(size=7000)
                return
            self.send_response(204)
            self.end_headers()

        def respond(self, body: bytes) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def trickle(self, *, size: int = 1000) -> None:
            body = json.dumps({"id": "owned", "padding": "x" * size}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                for offset in range(0, len(body), 20):
                    self.wfile.write(body[offset:offset + 20])
                    self.wfile.flush()
                    time.sleep(0.02)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                disconnected.set()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    address = f"http://127.0.0.1:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield address, methods, disconnected
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("sql", ["initial", "paged"])
def test_slow_body_respects_total_deadline_and_closes_response(
    slow_trino: tuple[str, list[str], threading.Event], sql: str,
) -> None:
    address, methods, disconnected = slow_trino
    with httpx.Client(trust_env=False) as http:
        client = TrinoClient(address, client=http, query_timeout=0.15)
        started = time.monotonic()
        with pytest.raises(TrinoQueryTimeoutError) as caught:
            client.query(sql)
        elapsed = time.monotonic() - started
        assert elapsed < 0.65, f"0.15-second query took {elapsed:.3f} seconds"
        assert caught.value.query_id == ("owned" if sql == "paged" else None)
        assert caught.value.cancellation == ("accepted" if sql == "paged" else "not_available")
        assert methods == (["POST", "GET", "DELETE"] if sql == "paged" else ["POST"])
        assert disconnected.wait(1), "client left the slow response open after timeout"


def test_slow_cancellation_body_keeps_its_separate_five_second_limit(
    slow_trino: tuple[str, list[str], threading.Event],
) -> None:
    address, methods, _ = slow_trino
    with httpx.Client(trust_env=False) as http:
        client = TrinoClient(address, client=http, query_timeout=0.15)
        started = time.monotonic()
        with pytest.raises(TrinoQueryTimeoutError) as caught:
            client.query("slow-cancel")
        elapsed = time.monotonic() - started
        assert elapsed < 6, f"query and cancellation took {elapsed:.3f} seconds"
        assert caught.value.query_id == "owned"
        assert caught.value.cancellation == "transport_failed"
        assert methods == ["POST", "GET", "DELETE"]


def test_slow_gold_query_does_not_checkpoint(
    slow_trino: tuple[str, list[str], threading.Event],
) -> None:
    address, _, disconnected = slow_trino
    planner = Planner()
    with httpx.Client(trust_env=False) as http:
        client = TrinoClient(address, client=http, query_timeout=0.15)
        with pytest.raises(pipeline.CommercePipelineError, match="verify-commerce-gold"):
            pipeline.run_commerce_batch(planner, client.query, run_stage=lambda *_: None)
        assert planner.commits == []
        assert disconnected.wait(1)


def test_unresponsive_transport_has_bounded_readers_and_recovers() -> None:
    from test_trino import final_stats

    release = threading.Event()
    finished = threading.Event()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) <= 2:
            assert release.wait(2)
            if len(requests) == 2:
                finished.set()
        return httpx.Response(200, json={"id": "done", "stats": final_stats()})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = TrinoClient("http://trino", client=http, query_timeout=0.03)
        try:
            for _ in range(3):
                with pytest.raises(TrinoQueryTimeoutError):
                    client.query("SELECT 1")
            assert len(requests) == 2
        finally:
            release.set()
        assert finished.wait(1)
        client._query_timeout = 1
        assert client.query("SELECT 1") == []
