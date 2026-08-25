from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import httpx

from lakehouse_ops.trino import TrinoClient
from lakehouse_ops.trino_upgrade import load_upgrade_plan, run_upgrade_rehearsal

SERVICES = ("trino-coordinator", "trino-worker", "trino-worker-2")


def wait_for_cluster(server: str, spec: dict[str, str]) -> dict[str, Any]:
    deadline = time.monotonic() + 150
    coordinator_id: str | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{server.rstrip('/')}/v1/info", timeout=3)
            response.raise_for_status()
            info = response.json()
            coordinator_id = info.get("coordinatorId")
            if info.get("starting") is not False:
                time.sleep(1)
                continue
            with TrinoClient(server, user="upgrade-readiness", timeout=5) as client:
                rows = client.query(
                    "SELECT count(*) AS active_nodes, "
                    "count_if(node_version = '"
                    f"{spec['version']}') AS expected_version_nodes "
                    "FROM system.runtime.nodes WHERE state = 'active'"
                )
            if (
                len(rows) == 1
                and rows[0].get("active_nodes") == 3
                and rows[0].get("expected_version_nodes") == 3
            ):
                return {
                    "active_nodes": 3,
                    "coordinator_id": coordinator_id,
                    "version": spec["version"],
                    "image": spec["image"],
                    "container_image_id": coordinator_image_id(),
                }
        except (httpx.HTTPError, ValueError, RuntimeError):
            pass
        time.sleep(2)
    raise RuntimeError(f"Trino {spec['version']} did not become ready")


def coordinator_image_id() -> str:
    container_id = subprocess.check_output(
        ["docker", "compose", "ps", "-q", "trino-coordinator"], text=True
    ).strip()
    if not container_id:
        raise RuntimeError("Trino coordinator container was not found")
    image_id = subprocess.check_output(
        ["docker", "inspect", "--format", "{{.Image}}", container_id], text=True
    ).strip()
    if not image_id.startswith("sha256:"):
        raise RuntimeError("Trino coordinator image ID is invalid")
    return image_id


def switch_cluster(server: str, spec: dict[str, str]) -> dict[str, Any]:
    environment = {**os.environ, "TRINO_SERVER_IMAGE": spec["image"]}
    subprocess.run(
        ["docker", "compose", "stop", "trino-worker", "trino-worker-2"],
        check=True,
        timeout=90,
        env=environment,
    )
    subprocess.run(
        ["docker", "compose", "stop", "trino-coordinator"],
        check=True,
        timeout=90,
        env=environment,
    )
    subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "query",
            "up",
            "-d",
            "--force-recreate",
            *SERVICES,
        ],
        check=True,
        timeout=180,
        env=environment,
    )
    return wait_for_cluster(server, spec)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("server")
    parser.add_argument("report", type=Path)
    parser.add_argument("plan", type=Path)
    args = parser.parse_args()

    plan = load_upgrade_plan(args.plan)
    initial = wait_for_cluster(args.server, plan["source"])
    report = run_upgrade_rehearsal(
        lambda: TrinoClient(args.server, user="upgrade-rehearsal", timeout=60),
        lambda spec: switch_cluster(args.server, spec),
        initial,
        plan,
    )
    args.report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
