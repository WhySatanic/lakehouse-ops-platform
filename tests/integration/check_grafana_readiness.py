from __future__ import annotations

import base64
import json
import os
import sys
import time
from urllib.request import Request, urlopen

DASHBOARD_UID = "lakehouse-core-readiness"
DATASOURCE_UID = "prometheus"


def _request_json(url: str, username: str, password: str) -> object:
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    request = Request(url, headers={"Authorization": f"Basic {token}"})
    with urlopen(request, timeout=5) as response:
        return json.load(response)


def dashboard_is_provisioned(search: object) -> bool:
    return isinstance(search, list) and any(
        isinstance(item, dict)
        and item.get("uid") == DASHBOARD_UID
        and item.get("title") == "Lakehouse Core Readiness"
        for item in search
    )


def main() -> int:
    server = os.getenv("GRAFANA_SERVER", "http://grafana:3000").rstrip("/")
    username = os.getenv("GRAFANA_ADMIN_USER", "admin")
    password = os.getenv("GRAFANA_ADMIN_PASSWORD", "lakeops-grafana-development-only")
    attempts = int(os.getenv("GRAFANA_CHECK_ATTEMPTS", "24"))
    delay = float(os.getenv("GRAFANA_CHECK_DELAY_SECONDS", "5"))
    last_error = "Grafana did not return provisioned resources"

    for _ in range(attempts):
        try:
            health = _request_json(f"{server}/api/health", username, password)
            search = _request_json(f"{server}/api/search?query=Lakehouse", username, password)
            datasource = _request_json(
                f"{server}/api/datasources/uid/{DATASOURCE_UID}/health", username, password
            )
            database_ok = isinstance(health, dict) and health.get("database") == "ok"
            datasource_ok = isinstance(datasource, dict) and datasource.get("status") == "OK"
            if database_ok and dashboard_is_provisioned(search) and datasource_ok:
                print("Grafana dashboard and Prometheus datasource are healthy")
                return 0
            last_error = (
                f"database_ok={database_ok}, "
                f"dashboard_ok={dashboard_is_provisioned(search)}, "
                f"datasource_ok={datasource_ok}"
            )
        except (OSError, ValueError, KeyError, TypeError) as error:
            last_error = str(error)
        time.sleep(delay)

    print(f"Grafana readiness check failed: {last_error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
