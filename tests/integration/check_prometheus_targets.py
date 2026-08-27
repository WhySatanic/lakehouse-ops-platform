from __future__ import annotations

import json
import os
import sys
import time
from urllib.parse import urlencode
from urllib.request import urlopen

EXPECTED_TARGETS = {
    "http://minio:9000/minio/health/live",
    "hive-metastore:9083",
    "http://trino-coordinator:8080/v1/info",
}


def successful_targets(payload: dict[str, object]) -> set[str]:
    if payload.get("status") != "success":
        return set()
    data = payload.get("data")
    if not isinstance(data, dict):
        return set()
    result = data.get("result")
    if not isinstance(result, list):
        return set()
    return {
        str(item["metric"]["instance"])
        for item in result
        if isinstance(item, dict)
        and isinstance(item.get("metric"), dict)
        and item.get("value", [None, "0"])[1] == "1"
    }


def main() -> int:
    server = os.getenv("PROMETHEUS_SERVER", "http://prometheus:9090").rstrip("/")
    attempts = int(os.getenv("PROMETHEUS_CHECK_ATTEMPTS", "24"))
    delay = float(os.getenv("PROMETHEUS_CHECK_DELAY_SECONDS", "5"))
    url = f"{server}/api/v1/query?{urlencode({'query': 'probe_success'})}"

    last_error = "no samples returned"
    for _ in range(attempts):
        try:
            with urlopen(url, timeout=5) as response:
                observed = successful_targets(json.load(response))
            missing = EXPECTED_TARGETS - observed
            if not missing:
                print("Prometheus reports all core readiness targets as healthy")
                return 0
            last_error = f"missing healthy targets: {', '.join(sorted(missing))}"
        except (OSError, ValueError, KeyError, IndexError, TypeError) as error:
            last_error = str(error)
        time.sleep(delay)

    print(f"Prometheus readiness check failed: {last_error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
