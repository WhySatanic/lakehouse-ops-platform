from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from math import isfinite
from threading import BoundedSemaphore, Event, Thread
from time import monotonic
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx


class TrinoQueryError(RuntimeError):
    pass


class TrinoProtocolError(RuntimeError):
    pass


class TrinoQueryTimeoutError(TrinoProtocolError):
    def __init__(self, query_id: str | None, cancellation: str) -> None:
        self.query_id = query_id
        self.cancellation = cancellation
        super().__init__(
            f"Trino query time budget exhausted: query_id={query_id}, "
            f"cancellation={cancellation}"
        )


@dataclass(frozen=True, slots=True)
class TrinoQueryStats:
    state: str
    elapsed_time_ms: int
    wall_time_ms: int
    cpu_time_ms: int
    processed_rows: int
    processed_bytes: int
    physical_input_bytes: int
    peak_memory_bytes: int
    spilled_bytes: int


@dataclass(frozen=True, slots=True)
class TrinoQueryResult:
    query_id: str
    rows: tuple[dict[str, Any], ...]
    stats: TrinoQueryStats


class _DeadlineStream(httpx.SyncByteStream):
    def __init__(self, stream: httpx.SyncByteStream, deadline: float, stopped: Event) -> None:
        self._stream = stream
        self._deadline = deadline
        self._stopped = stopped

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self._stream:
            if self._stopped.is_set() or monotonic() >= self._deadline:
                raise httpx.ReadTimeout("Trino response exceeded the query deadline")
            yield chunk

    def close(self) -> None:
        self._stream.close()


class TrinoClient:
    def __init__(
        self,
        server: str,
        *,
        user: str = "lakehouse-ops",
        timeout: float = 30.0,
        query_timeout: float | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        if query_timeout is not None and (
            isinstance(query_timeout, bool)
            or not isfinite(query_timeout)
            or query_timeout <= 0
        ):
            raise ValueError("Trino query_timeout must be finite and positive")
        parsed = urlsplit(server.rstrip("/"))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Trino server must be an absolute HTTP(S) URL")
        self._server = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
        self._origin = (parsed.scheme, parsed.netloc)
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout)
        self._query_timeout = query_timeout
        # One request and its cancellation may be closing at the same time.
        # A stalled transport must not create an unbounded number of reader threads.
        self._request_slots = BoundedSemaphore(2)
        self._headers = {
            "Accept": "application/json",
            "X-Trino-User": user,
            "X-Trino-Source": "lakehouse-ops",
            "X-Trino-Time-Zone": "UTC",
        }

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> TrinoClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def query(self, sql: str) -> list[dict[str, Any]]:
        return list(self.query_with_stats(sql).rows)

    def query_with_stats(self, sql: str) -> TrinoQueryResult:
        deadline = (
            None if self._query_timeout is None else monotonic() + self._query_timeout
        )
        query_id: str | None = None
        cancellation_uri: str | None = None

        def request(method: str, url: str, **kwargs: Any) -> httpx.Response:
            if deadline is not None:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    self._query_timeout_error(query_id, cancellation_uri)
                # Preserve shorter caller-configured phase limits, including None.
                configured = self._client.timeout
                kwargs["timeout"] = httpx.Timeout(
                    **{
                        phase: remaining if value is None else min(value, remaining)
                        for phase, value in configured.as_dict().items()
                    }
                )
            try:
                if deadline is None:
                    return self._client.request(method, url, **kwargs)
                return self._request_until(method, url, deadline, **kwargs)
            except httpx.TimeoutException:
                if deadline is not None:
                    self._query_timeout_error(query_id, cancellation_uri)
                raise

        response = request(
            "POST",
            f"{self._server}/v1/statement",
            content=sql,
            headers={**self._headers, "Content-Type": "text/plain; charset=utf-8"},
        )
        columns: list[str] | None = None
        rows: list[dict[str, Any]] = []

        for _ in range(1000):
            payload = self._payload(response)
            current_id = payload.get("id")
            if not isinstance(current_id, str) or not current_id:
                raise TrinoProtocolError("Trino response has no query ID")
            if query_id is not None and current_id != query_id:
                raise TrinoProtocolError("Trino query ID changed between result pages")
            query_id = current_id
            next_uri = payload.get("nextUri")
            cancellation_uri = self._coordinator_uri(next_uri) if next_uri else None
            if deadline is not None and monotonic() >= deadline:
                self._query_timeout_error(query_id, cancellation_uri)
            if error := payload.get("error"):
                name = error.get("errorName", "TRINO_QUERY_ERROR")
                message = error.get("message", "query failed without a message")
                raise TrinoQueryError(f"{name}: {message}")

            if "columns" in payload:
                columns = [column["name"] for column in payload["columns"]]
            if data := payload.get("data"):
                if columns is None:
                    raise TrinoProtocolError("Trino returned data before column metadata")
                for values in data:
                    if len(values) != len(columns):
                        raise TrinoProtocolError("Trino row width does not match columns")
                    rows.append(dict(zip(columns, values, strict=True)))

            if not next_uri:
                return TrinoQueryResult(
                    query_id=query_id,
                    rows=tuple(rows),
                    stats=_query_stats(payload.get("stats")),
                )
            response = request("GET", cancellation_uri, headers=self._headers)

        raise TrinoProtocolError("Trino query exceeded the 1000-page safety limit")

    def _request_until(
        self, method: str, url: str, deadline: float, **kwargs: Any,
    ) -> httpx.Response:
        remaining = deadline - monotonic()
        if remaining <= 0 or not self._request_slots.acquire(timeout=remaining):
            raise httpx.ReadTimeout("Trino HTTP request exceeded its deadline")
        stopped = Event()
        future: Future[httpx.Response] = Future()

        def read() -> None:
            try:
                if stopped.is_set() or monotonic() >= deadline:
                    raise httpx.ReadTimeout("Trino HTTP request exceeded its deadline")
                with self._client.stream(method, url, **kwargs) as response:
                    response.stream = _DeadlineStream(response.stream, deadline, stopped)
                    response.read()
                    future.set_result(response)
            except BaseException as error:
                future.set_exception(error)
            finally:
                self._request_slots.release()

        try:
            Thread(target=read, name="trino-http-read", daemon=True).start()
        except BaseException:
            self._request_slots.release()
            raise
        try:
            return future.result(timeout=max(0, deadline - monotonic()))
        except FutureTimeoutError:
            raise httpx.ReadTimeout("Trino HTTP request exceeded its deadline") from None
        finally:
            # The reader owns and closes its response, including after caller expiry.
            # Phase timeouts bound a blocked read; chunks never keep it alive forever.
            stopped.set()

    def _query_timeout_error(self, query_id: str | None, uri: str | None) -> None:
        cancellation = "not_available"
        if uri is not None:
            try:
                response = self._request_until(
                    "DELETE", uri, monotonic() + 5.0, headers=self._headers, timeout=5.0,
                )
                cancellation = (
                    "accepted" if response.is_success else f"http_{response.status_code}"
                )
            except httpx.HTTPError:
                cancellation = "transport_failed"
        raise TrinoQueryTimeoutError(query_id, cancellation)

    @staticmethod
    def _payload(response: httpx.Response) -> dict[str, Any]:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            raise TrinoProtocolError(
                f"Trino returned HTTP {response.status_code}"
            ) from error
        try:
            payload = response.json()
        except ValueError as error:
            raise TrinoProtocolError("Trino returned invalid JSON") from error
        if not isinstance(payload, dict):
            raise TrinoProtocolError("Trino response must be a JSON object")
        return payload

    def _coordinator_uri(self, next_uri: str) -> str:
        parsed = urlsplit(next_uri)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise TrinoProtocolError("Trino nextUri must be an absolute HTTP(S) URL")
        if not parsed.path.startswith("/v1/statement/"):
            raise TrinoProtocolError("Trino nextUri has an unexpected path")
        return urlunsplit((*self._origin, parsed.path, parsed.query, ""))


def _query_stats(value: Any) -> TrinoQueryStats:
    if not isinstance(value, dict):
        raise TrinoProtocolError("Trino final response has no query statistics")
    state = value.get("state")
    if state != "FINISHED":
        raise TrinoProtocolError(f"Trino final query state is not FINISHED: {state}")
    return TrinoQueryStats(
        state=state,
        elapsed_time_ms=_non_negative_integer(value, "elapsedTimeMillis"),
        wall_time_ms=_non_negative_integer(value, "wallTimeMillis"),
        cpu_time_ms=_non_negative_integer(value, "cpuTimeMillis"),
        processed_rows=_non_negative_integer(value, "processedRows"),
        processed_bytes=_non_negative_integer(value, "processedBytes"),
        physical_input_bytes=_non_negative_integer(value, "physicalInputBytes"),
        peak_memory_bytes=_non_negative_integer(value, "peakMemoryBytes"),
        spilled_bytes=_non_negative_integer(value, "spilledBytes"),
    )


def _non_negative_integer(value: dict[str, Any], key: str) -> int:
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        raise TrinoProtocolError(f"Trino query statistic {key} must be non-negative")
    return raw
