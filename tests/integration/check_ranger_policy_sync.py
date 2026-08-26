from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    policies = report.get("policies", {})
    users = report.get("users", {})

    if report.get("status") != "synchronized":
        raise ValueError("Ranger synchronization did not succeed")
    if report.get("service_status") != "unchanged":
        raise ValueError("Ranger service changed on the idempotence pass")
    if any(policies.get(key) != 0 for key in ("created", "updated", "deleted")):
        raise ValueError("Ranger policies changed on the idempotence pass")
    if policies.get("unchanged") != policies.get("desired"):
        raise ValueError("Ranger policy set is incomplete")
    if users.get("created") != 0 or users.get("unchanged") != users.get("desired"):
        raise ValueError("Ranger users changed on the idempotence pass")

    print(
        json.dumps(
            {
                "status": "ready",
                "service": report.get("service"),
                "policies": policies.get("desired"),
                "users": users.get("desired"),
            }
        )
    )


if __name__ == "__main__":
    main()
