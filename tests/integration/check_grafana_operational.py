from __future__ import annotations

import base64
import json
import math
import os
import sys
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DASHBOARD_UID = "lakehouse-maintenance-freshness"
DASHBOARD_TITLE = "Lakehouse Maintenance and Freshness"
DATASOURCE_UID = "prometheus"
METRICS = {
    "collector": ("lakehouse_operational_collector_success", 1),
    "freshness": ("lakehouse_ingestion_freshness_age_seconds", 0),
    "data_files": ("lakehouse_maintenance_data_files", 1),
    "small_files": ("lakehouse_maintenance_small_file_backlog", 0),
    "snapshots": ("lakehouse_maintenance_snapshots", 1),
}


def _request_json(url: str, username: str | None = None, password: str = "") -> object:
    headers: dict[str, str] = {}
    if username is not None:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    with urlopen(Request(url, headers=headers), timeout=5) as response:
        return json.load(response)


def dashboard_is_provisioned(search: object) -> bool:
    return isinstance(search, list) and any(
        isinstance(item, dict)
        and item.get("uid") == DASHBOARD_UID
        and item.get("title") == DASHBOARD_TITLE
        for item in search
    )


def sample_at_least(payload: object, minimum: float) -> bool:
    if not isinstance(payload, dict) or payload.get("status") != "success":
        return False
    data = payload.get("data")
    if not isinstance(data, dict):
        return False
    result = data.get("result")
    if not isinstance(result, list) or not result:
        return False
    for item in result:
        if not isinstance(item, dict):
            continue
        value = item.get("value")
        if not isinstance(value, list) or len(value) != 2:
            continue
        try:
            sample = float(value[1])
        except (TypeError, ValueError):
            continue
        if math.isfinite(sample) and sample >= minimum:
            return True
    return False


def main() -> int:
    grafana = os.getenv("GRAFANA_SERVER", "http://grafana:3000").rstrip("/")
    prometheus = os.getenv("PROMETHEUS_SERVER", "http://prometheus:9090").rstrip("/")
    username = os.getenv("GRAFANA_ADMIN_USER", "admin")
    password = os.getenv("GRAFANA_ADMIN_PASSWORD", "lakeops-grafana-development-only")
    attempts = int(os.getenv("GRAFANA_CHECK_ATTEMPTS", "24"))
    delay = float(os.getenv("GRAFANA_CHECK_DELAY_SECONDS", "5"))
    last_error = "Grafana operational resources are not ready"

    for _ in range(attempts):
        try:
            search = _request_json(
                f"{grafana}/api/search?query=Lakehouse", username, password
            )
            datasource = _request_json(
                f"{grafana}/api/datasources/uid/{DATASOURCE_UID}/health",
                username,
                password,
            )
            up = _request_json(
                f"{prometheus}/api/v1/query?"
                f"{urlencode({'query': 'up{job="lakehouse-operational"}'})}"
            )
            samples = {
                name: _request_json(
                    f"{prometheus}/api/v1/query?{urlencode({'query': query})}"
                )
                for name, (query, _) in METRICS.items()
            }
            dashboard_ok = dashboard_is_provisioned(search)
            datasource_ok = isinstance(datasource, dict) and datasource.get("status") == "OK"
            target_ok = sample_at_least(up, 1)
            metrics_ok = all(
                sample_at_least(samples[name], minimum)
                for name, (_, minimum) in METRICS.items()
            )
            if dashboard_ok and datasource_ok and target_ok and metrics_ok:
                print("Grafana maintenance and freshness dashboard has live metrics")
                return 0
            last_error = (
                f"dashboard_ok={dashboard_ok}, datasource_ok={datasource_ok}, "
                f"target_ok={target_ok}, metrics_ok={metrics_ok}"
            )
        except (OSError, ValueError, KeyError, TypeError) as error:
            last_error = str(error)
        time.sleep(delay)

    print(f"Grafana operational check failed: {last_error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
