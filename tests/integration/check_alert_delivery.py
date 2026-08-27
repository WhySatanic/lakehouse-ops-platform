from __future__ import annotations

import json
import os
import sys
import time
from urllib.request import urlopen

ALERT_NAME = "LakehouseCoreTargetDown"
DEFAULT_TARGET = "http://trino-coordinator:8080/v1/info"


def has_matching_alert(events: object, status: str, target: str) -> bool:
    if not isinstance(events, list):
        return False
    for event in events:
        if not isinstance(event, dict) or not isinstance(event.get("alerts"), list):
            continue
        for alert in event["alerts"]:
            labels = alert.get("labels", {}) if isinstance(alert, dict) else {}
            if (
                isinstance(labels, dict)
                and alert.get("status") == status
                and labels.get("alertname") == ALERT_NAME
                and labels.get("instance") == target
            ):
                return True
    return False


def main() -> int:
    server = os.getenv("ALERT_WEBHOOK_SERVER", "http://alert-webhook:8080").rstrip("/")
    expected_status = os.getenv("EXPECTED_ALERT_STATUS", "firing")
    expected_target = os.getenv("EXPECTED_ALERT_TARGET", DEFAULT_TARGET)
    attempts = int(os.getenv("ALERT_CHECK_ATTEMPTS", "30"))
    delay = float(os.getenv("ALERT_CHECK_DELAY_SECONDS", "5"))
    last_error = "no matching alert delivered"

    for _ in range(attempts):
        try:
            with urlopen(f"{server}/events", timeout=5) as response:
                events = json.load(response)
            if has_matching_alert(events, expected_status, expected_target):
                print(f"Alertmanager delivered {expected_status} alert for {expected_target}")
                return 0
        except (OSError, ValueError, KeyError, TypeError) as error:
            last_error = str(error)
        time.sleep(delay)

    print(
        f"Alert delivery check failed for status={expected_status}, "
        f"target={expected_target}: {last_error}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
