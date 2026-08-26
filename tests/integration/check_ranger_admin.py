from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

import httpx

REQUIRED_RESOURCES = {
    "catalog",
    "schema",
    "table",
    "column",
    "trinouser",
    "queryid",
    "sysinfo",
}
REQUIRED_ACCESS_TYPES = {
    "select",
    "execute",
    "impersonate",
    "read_sysinfo",
    "write_sysinfo",
}


def validate_service_definition(document: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if document.get("name") != "trino":
        errors.append("name")
    if document.get("implClass") != "org.apache.ranger.services.trino.RangerServiceTrino":
        errors.append("implClass")
    resources = document.get("resources")
    if not isinstance(resources, list):
        errors.append("resources")
    elif not REQUIRED_RESOURCES.issubset(
        {item.get("name") for item in resources if isinstance(item, dict)}
    ):
        errors.append("resources.coverage")
    access_types = document.get("accessTypes")
    if not isinstance(access_types, list):
        errors.append("accessTypes")
    elif not REQUIRED_ACCESS_TYPES.issubset(
        {item.get("name") for item in access_types if isinstance(item, dict)}
    ):
        errors.append("accessTypes.coverage")
    return errors


def fetch_service_definition(
    base_url: str, username: str, password: str, *, attempts: int, delay_seconds: float
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/service/plugins/definitions/name/trino"
    last_error: Exception | None = None
    with httpx.Client(auth=(username, password), timeout=10) as client:
        for attempt in range(attempts):
            try:
                response = client.get(url)
                response.raise_for_status()
                document = response.json()
                if not isinstance(document, dict):
                    raise ValueError("Ranger service definition must be a JSON object")
                return document
            except (httpx.HTTPError, json.JSONDecodeError, ValueError) as error:
                last_error = error
                if attempt + 1 < attempts:
                    time.sleep(delay_seconds)
    raise RuntimeError(f"Ranger Trino service definition unavailable: {last_error}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:6080")
    parser.add_argument("--username", default=os.getenv("RANGER_ADMIN_USER", "admin"))
    parser.add_argument(
        "--attempts", type=int, default=30, help="bounded readiness attempts"
    )
    parser.add_argument("--delay-seconds", type=float, default=2)
    args = parser.parse_args()
    password = os.getenv("RANGER_ADMIN_PASSWORD", "rangerR0cks!")
    document = fetch_service_definition(
        args.url,
        args.username,
        password,
        attempts=args.attempts,
        delay_seconds=args.delay_seconds,
    )
    errors = validate_service_definition(document)
    if errors:
        raise SystemExit(f"Ranger Trino service definition failed: {', '.join(errors)}")
    print(
        json.dumps(
            {
                "schema_version": "1.0",
                "status": "ready",
                "deployment": "apache/ranger:2.9.0",
                "service_definition": "trino",
                "required_resources": sorted(REQUIRED_RESOURCES),
                "required_access_types": sorted(REQUIRED_ACCESS_TYPES),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
