from __future__ import annotations

import argparse
import json
import ssl
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from lakehouse_ops.trino import TrinoClient, TrinoQueryError


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="http://localhost:8080")
    parser.add_argument("--user", default="incident-responder")
    parser.add_argument("--expect", choices=("allowed", "denied"), required=True)
    parser.add_argument("--attempts", type=int, default=12)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument(
        "--mode", choices=("default", "authenticated-ranger"), default="default"
    )
    parser.add_argument("--password")
    parser.add_argument("--ca-cert", type=Path)
    args = parser.parse_args()

    client: httpx.Client | None = None
    security = None
    if args.mode == "authenticated-ranger":
        if not args.password:
            raise ValueError("--password is required for authenticated Ranger access")
        if args.ca_cert is None or not args.ca_cert.is_file():
            raise ValueError("--ca-cert must name the trusted Trino certificate")
        if urlsplit(args.server).scheme != "https":
            raise ValueError("authenticated Ranger access requires an HTTPS server")
        client = httpx.Client(
            auth=(args.user, args.password),
            timeout=45,
            verify=ssl.create_default_context(cafile=str(args.ca_cert)),
        )
        security = {
            "authentication": "password",
            "authorization": "ranger",
            "query_user": args.user,
            "tls_verified": True,
            "transport": "https",
        }

    actual = "unknown"
    detail = "policy did not converge"
    started_at = datetime.now(UTC).isoformat()
    try:
        for _ in range(args.attempts):
            query_id = None
            try:
                with TrinoClient(args.server, user=args.user, client=client) as trino:
                    result = trino.query_with_stats(
                        "SELECT count(*) AS row_count "
                        "FROM lakehouse.bronze.weather_hourly"
                    )
                actual = "allowed"
                detail = str(result.rows)
                valid = result.rows == ({"row_count": 4},)
                query_id = result.query_id
            except TrinoQueryError as error:
                actual = "denied"
                detail = str(error)
                valid = "Access Denied" in detail or "PERMISSION_DENIED" in detail
            if actual == args.expect and valid:
                report = {
                    "schema_version": "1.0",
                    "status": "ready",
                    "user": args.user,
                    "expectation": args.expect,
                    "result": actual,
                    "started_at": started_at,
                    "observed_at": datetime.now(UTC).isoformat(),
                }
                if query_id:
                    report["query_id"] = query_id
                if security:
                    report["security"] = security
                print(json.dumps(report, sort_keys=True))
                return
            time.sleep(args.interval)
    finally:
        if client is not None:
            client.close()
    raise SystemExit(
        f"break-glass access did not converge to {args.expect}: {actual}: {detail}"
    )


if __name__ == "__main__":
    main()
