from __future__ import annotations

import json
import math
import os
import sys
import time
from urllib.parse import urlencode
from urllib.request import urlopen

OBJECTIVES = {
    "lakehouse:slo:query_traffic_observed5m": (1.0, 1.0),
    "lakehouse:slo:query_success_ratio5m": (0.99, 1.0),
    "lakehouse:slo:ingestion_freshness_compliant": (1.0, 1.0),
    "lakehouse:slo:maintenance_backlog_compliant": (1.0, 1.0),
    "lakehouse:slo:objectives_met": (1.0, 1.0),
}


def metric_values(payload: object) -> list[float]:
    if not isinstance(payload, dict) or payload.get("status") != "success":
        return []
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    result = data.get("result")
    if not isinstance(result, list):
        return []
    values: list[float] = []
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
        if math.isfinite(sample):
            values.append(sample)
    return values


def objective_is_met(payload: object, minimum: float, maximum: float) -> bool:
    values = metric_values(payload)
    return bool(values) and all(minimum <= value <= maximum for value in values)


def _query(server: str, expression: str) -> object:
    url = f"{server}/api/v1/query?{urlencode({'query': expression})}"
    with urlopen(url, timeout=5) as response:
        return json.load(response)


def main() -> int:
    server = os.getenv("PROMETHEUS_SERVER", "http://prometheus:9090").rstrip("/")
    attempts = int(os.getenv("SLO_CHECK_ATTEMPTS", "24"))
    delay = float(os.getenv("SLO_CHECK_DELAY_SECONDS", "5"))
    last_status: dict[str, object] = {}

    for _ in range(attempts):
        try:
            last_status = {
                metric: _query(server, metric) for metric in OBJECTIVES
            }
            if all(
                objective_is_met(last_status[metric], minimum, maximum)
                for metric, (minimum, maximum) in OBJECTIVES.items()
            ):
                print("Platform query, freshness, and maintenance SLOs are met")
                return 0
        except (OSError, ValueError, KeyError, TypeError) as error:
            last_status = {"error": str(error)}
        time.sleep(delay)

    observed = {
        metric: metric_values(last_status.get(metric)) for metric in OBJECTIVES
    }
    print(f"Platform SLO check failed: {observed}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
