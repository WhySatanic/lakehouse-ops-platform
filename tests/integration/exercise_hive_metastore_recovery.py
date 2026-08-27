from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

from lakehouse_ops.metastore_recovery import (
    run_metastore_recovery,
    write_metastore_recovery_report,
)
from lakehouse_ops.trino import TrinoClient


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
    args = parser.parse_args()

    report = run_metastore_recovery(
        lambda: TrinoClient(args.server, user="lakehouse-recovery-drill", timeout=45),
        service_state,
        stop_metastore,
        start_metastore,
    )
    write_metastore_recovery_report(args.report, report)
    print(args.report.read_text(encoding="utf-8").strip())


if __name__ == "__main__":
    main()
