from __future__ import annotations

import argparse
import json
import ssl
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from lakehouse_ops.metastore_recovery import (
    AUTHENTICATED_RANGER_SECURITY,
    run_metastore_recovery,
    write_metastore_recovery_report,
)
from lakehouse_ops.trino import TrinoClient, TrinoQueryResult


class AuthenticatedTrinoClient:
    def __init__(self, server: str, password: str, ca_cert: Path) -> None:
        self._transport = httpx.Client(
            auth=("platform_admin", password),
            timeout=45,
            verify=ssl.create_default_context(cafile=str(ca_cert)),
        )
        self._client = TrinoClient(
            server,
            user="platform_admin",
            client=self._transport,
        )

    def query_with_stats(self, sql: str) -> TrinoQueryResult:
        return self._client.query_with_stats(sql)

    def close(self) -> None:
        self._transport.close()


def container_state(service: str) -> tuple[str, bool]:
    container_id = subprocess.check_output(
        ["docker", "compose", "ps", "--all", "-q", service], text=True
    ).strip()
    if not container_id:
        raise RuntimeError(f"Compose container not found: {service}")
    raw = subprocess.check_output(
        ["docker", "inspect", "--format", "{{json .State}}", container_id],
        text=True,
    )
    state = json.loads(raw)
    return container_id, state.get("Running") is True


def service_state() -> dict[str, Any]:
    metastore_id, metastore_running = container_state("hive-metastore")
    database_id, database_running = container_state("metastore-db")
    return {
        "metastore_container_id": metastore_id,
        "metastore_running": metastore_running,
        "database_container_id": database_id,
        "database_running": database_running,
    }


def stop_metastore() -> None:
    subprocess.run(
        ["docker", "compose", "stop", "hive-metastore"],
        check=True,
        timeout=90,
    )


def start_metastore() -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "catalog",
            "up",
            "-d",
            "--wait",
            "hive-metastore",
        ],
        check=True,
        timeout=180,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("server")
    parser.add_argument("report", type=Path)
    parser.add_argument(
        "--mode",
        choices=("default", "authenticated-ranger"),
        default="default",
    )
    parser.add_argument("--password")
    parser.add_argument("--ca-cert", type=Path)
    args = parser.parse_args()

    security = None
    if args.mode == "authenticated-ranger":
        if not args.password:
            raise ValueError("--password is required for authenticated Ranger recovery")
        if args.ca_cert is None or not args.ca_cert.is_file():
            raise ValueError("--ca-cert must name the trusted Trino certificate")
        if urlsplit(args.server).scheme != "https":
            raise ValueError("authenticated Ranger recovery requires an HTTPS server")
        security = AUTHENTICATED_RANGER_SECURITY

    def client_factory() -> AuthenticatedTrinoClient | TrinoClient:
        if args.mode == "authenticated-ranger":
            assert args.password is not None
            assert args.ca_cert is not None
            return AuthenticatedTrinoClient(args.server, args.password, args.ca_cert)
        return TrinoClient(
            args.server, user="lakehouse-recovery-drill", timeout=45
        )

    report = run_metastore_recovery(
        client_factory,
        service_state,
        stop_metastore,
        start_metastore,
        security=security,
    )
    write_metastore_recovery_report(args.report, report)
    print(args.report.read_text(encoding="utf-8").strip())


if __name__ == "__main__":
    main()
