from __future__ import annotations

import argparse
import json
import time

from lakehouse_ops.trino import TrinoClient, TrinoQueryError


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="http://localhost:8080")
    parser.add_argument("--user", default="incident-responder")
    parser.add_argument("--expect", choices=("allowed", "denied"), required=True)
    parser.add_argument("--attempts", type=int, default=12)
    parser.add_argument("--interval", type=float, default=5)
    args = parser.parse_args()

    actual = "unknown"
    detail = "policy did not converge"
    for _ in range(args.attempts):
        try:
            with TrinoClient(args.server, user=args.user) as client:
                rows = client.query(
                    "SELECT count(*) AS row_count FROM lakehouse.bronze.weather_hourly"
                )
            actual = "allowed"
            detail = str(rows)
            valid = rows == [{"row_count": 4}]
        except TrinoQueryError as error:
            actual = "denied"
            detail = str(error)
            valid = "Access Denied" in detail or "PERMISSION_DENIED" in detail
        if actual == args.expect and valid:
            print(
                json.dumps(
                    {
                        "schema_version": "1.0",
                        "status": "ready",
                        "user": args.user,
                        "expectation": args.expect,
                        "result": actual,
                    },
                    sort_keys=True,
                )
            )
            return
        time.sleep(args.interval)
    raise SystemExit(
        f"break-glass access did not converge to {args.expect}: {actual}: {detail}"
    )


if __name__ == "__main__":
    main()
