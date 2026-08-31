from __future__ import annotations

import json
import math
import os
import time
from collections.abc import Iterable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.request import Request, urlopen

TABLE = "lakehouse.silver.weather_hourly"
TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024
OPERATIONAL_SQL = f"""
SELECT
  count(*) AS row_count,
  to_unixtime(max(ingested_at)) AS latest_ingested_at,
  (SELECT count(*) FROM lakehouse.silver."weather_hourly$files") AS data_file_count,
  (SELECT count_if(file_size_in_bytes < {TARGET_FILE_SIZE_BYTES})
   FROM lakehouse.silver."weather_hourly$files") AS small_file_count,
  (SELECT count(*) FROM lakehouse.silver."weather_hourly$snapshots") AS snapshot_count
FROM {TABLE}
""".strip()


class OperationalMetricsError(ValueError):
    pass


@dataclass(frozen=True)
class OperationalSnapshot:
    row_count: int
    latest_ingested_at: float
    data_file_count: int
    small_file_count: int
    snapshot_count: int


def parse_operational_snapshot(pages: Iterable[dict[str, Any]]) -> OperationalSnapshot:
    column_names: list[str] | None = None
    rows: list[list[Any]] = []
    for page in pages:
        error = page.get("error")
        if isinstance(error, dict):
            name = error.get("errorName", "TRINO_QUERY_ERROR")
            message = error.get("message", "query failed")
            raise OperationalMetricsError(f"{name}: {message}")
        columns = page.get("columns")
        if isinstance(columns, list):
            column_names = [
                str(column["name"])
                for column in columns
                if isinstance(column, dict) and "name" in column
            ]
        data = page.get("data")
        if isinstance(data, list):
            rows.extend(row for row in data if isinstance(row, list))

    if column_names is None or len(rows) != 1 or len(column_names) != len(rows[0]):
        raise OperationalMetricsError("Trino returned an unexpected operational snapshot")
    values = dict(zip(column_names, rows[0], strict=True))
    snapshot = OperationalSnapshot(
        row_count=_positive_int(values.get("row_count"), "row_count"),
        latest_ingested_at=_positive_float(
            values.get("latest_ingested_at"), "latest_ingested_at"
        ),
        data_file_count=_positive_int(values.get("data_file_count"), "data_file_count"),
        small_file_count=_non_negative_int(
            values.get("small_file_count"), "small_file_count"
        ),
        snapshot_count=_positive_int(values.get("snapshot_count"), "snapshot_count"),
    )
    if snapshot.small_file_count > snapshot.data_file_count:
        raise OperationalMetricsError("small_file_count exceeds data_file_count")
    return snapshot


def fetch_operational_snapshot(server: str, user: str, timeout: float) -> OperationalSnapshot:
    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "X-Trino-User": user,
        "X-Trino-Source": "lakehouse-operational-metrics",
        "X-Trino-Time-Zone": "UTC",
    }
    request = Request(
        f"{server.rstrip('/')}/v1/statement",
        data=OPERATIONAL_SQL.encode(),
        headers=headers,
        method="POST",
    )
    pages: list[dict[str, Any]] = []
    while True:
        with urlopen(request, timeout=timeout) as response:
            page = json.load(response)
        if not isinstance(page, dict):
            raise OperationalMetricsError("Trino response is not a JSON object")
        pages.append(page)
        next_uri = page.get("nextUri")
        if next_uri is None:
            break
        if not isinstance(next_uri, str) or not next_uri.startswith(("http://", "https://")):
            raise OperationalMetricsError("Trino next URI is invalid")
        request = Request(next_uri, headers=headers)
    return parse_operational_snapshot(pages)


def render_prometheus(snapshot: OperationalSnapshot, collected_at: float) -> str:
    if not math.isfinite(collected_at) or collected_at <= 0:
        raise OperationalMetricsError("collection timestamp is invalid")
    freshness = max(0.0, collected_at - snapshot.latest_ingested_at)
    label = f'table="{TABLE}"'
    return "\n".join(
        [
            "# HELP lakehouse_operational_collector_success "
            "Whether the latest Trino collection succeeded.",
            "# TYPE lakehouse_operational_collector_success gauge",
            "lakehouse_operational_collector_success 1",
            "# HELP lakehouse_ingestion_rows Current rows in the observed table.",
            "# TYPE lakehouse_ingestion_rows gauge",
            f"lakehouse_ingestion_rows{{{label}}} {snapshot.row_count}",
            "# HELP lakehouse_ingestion_latest_timestamp_seconds "
            "Latest ingestion timestamp as Unix seconds.",
            "# TYPE lakehouse_ingestion_latest_timestamp_seconds gauge",
            f"lakehouse_ingestion_latest_timestamp_seconds{{{label}}} "
            f"{snapshot.latest_ingested_at:.3f}",
            "# HELP lakehouse_ingestion_freshness_age_seconds "
            "Age of the latest ingestion in seconds.",
            "# TYPE lakehouse_ingestion_freshness_age_seconds gauge",
            f"lakehouse_ingestion_freshness_age_seconds{{{label}}} {freshness:.3f}",
            "# HELP lakehouse_maintenance_data_files Current Iceberg data-file count.",
            "# TYPE lakehouse_maintenance_data_files gauge",
            f"lakehouse_maintenance_data_files{{{label}}} {snapshot.data_file_count}",
            "# HELP lakehouse_maintenance_small_file_backlog "
            "Data files below the configured target size.",
            "# TYPE lakehouse_maintenance_small_file_backlog gauge",
            f"lakehouse_maintenance_small_file_backlog{{{label}}} {snapshot.small_file_count}",
            "# HELP lakehouse_maintenance_snapshots Current Iceberg snapshot count.",
            "# TYPE lakehouse_maintenance_snapshots gauge",
            f"lakehouse_maintenance_snapshots{{{label}}} {snapshot.snapshot_count}",
            "",
        ]
    )


def render_failure() -> str:
    return "\n".join(
        [
            "# HELP lakehouse_operational_collector_success "
            "Whether the latest Trino collection succeeded.",
            "# TYPE lakehouse_operational_collector_success gauge",
            "lakehouse_operational_collector_success 0",
            "",
        ]
    )


def make_handler(server: str, user: str, timeout: float) -> type[BaseHTTPRequestHandler]:
    class OperationalMetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/-/healthy":
                self._respond(200, "text/plain; charset=utf-8", "ok\n")
                return
            if self.path != "/metrics":
                self._respond(404, "text/plain; charset=utf-8", "not found\n")
                return
            try:
                snapshot = fetch_operational_snapshot(server, user, timeout)
                body = render_prometheus(snapshot, time.time())
            except (OSError, ValueError, KeyError, TypeError) as error:
                self.log_error("operational collection failed: %s", error)
                body = render_failure()
            self._respond(200, "text/plain; version=0.0.4; charset=utf-8", body)

        def _respond(self, status: int, content_type: str, body: str) -> None:
            encoded = body.encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: Any) -> None:
            return

    return OperationalMetricsHandler


def main() -> None:
    host = os.getenv("LAKEHOUSE_METRICS_HOST", "0.0.0.0")
    port = int(os.getenv("LAKEHOUSE_METRICS_PORT", "9108"))
    server = os.getenv("TRINO_SERVER", "http://trino-coordinator:8080")
    user = os.getenv("TRINO_USER", "lakehouse-ops")
    timeout = float(os.getenv("TRINO_METRICS_TIMEOUT_SECONDS", "10"))
    ThreadingHTTPServer((host, port), make_handler(server, user, timeout)).serve_forever()


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise OperationalMetricsError(f"{name} is invalid")
    return value


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise OperationalMetricsError(f"{name} is invalid")
    return value


def _positive_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OperationalMetricsError(f"{name} is invalid")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise OperationalMetricsError(f"{name} is invalid")
    return result


if __name__ == "__main__":
    main()
