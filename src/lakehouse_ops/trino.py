from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx


class TrinoQueryError(RuntimeError):
    pass


class TrinoProtocolError(RuntimeError):
    pass


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


class TrinoClient:
    def __init__(
        self,
        server: str,
        *,
        user: str = "lakehouse-ops",
        timeout: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        parsed = urlsplit(server.rstrip("/"))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Trino server must be an absolute HTTP(S) URL")
        self._server = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
        self._origin = (parsed.scheme, parsed.netloc)
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout)
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
        response = self._client.post(
            f"{self._server}/v1/statement",
            content=sql,
            headers={**self._headers, "Content-Type": "text/plain; charset=utf-8"},
        )
        columns: list[str] | None = None
        rows: list[dict[str, Any]] = []
        query_id: str | None = None

        for _ in range(1000):
            payload = self._payload(response)
            current_id = payload.get("id")
            if not isinstance(current_id, str) or not current_id:
                raise TrinoProtocolError("Trino response has no query ID")
            if query_id is not None and current_id != query_id:
                raise TrinoProtocolError("Trino query ID changed between result pages")
            query_id = current_id
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

            next_uri = payload.get("nextUri")
            if not next_uri:
                return TrinoQueryResult(
                    query_id=query_id,
                    rows=tuple(rows),
                    stats=_query_stats(payload.get("stats")),
                )
            response = self._client.get(
                self._coordinator_uri(next_uri), headers=self._headers
            )

        raise TrinoProtocolError("Trino query exceeded the 1000-page safety limit")

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
