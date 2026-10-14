"""Run three linked snapshots on a fresh scoped Docker reference stack."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from contextlib import ExitStack
from pathlib import Path
from typing import Any

PORT_NAMES = ("MINIO_API_PORT", "MINIO_CONSOLE_PORT", "HIVE_METASTORE_PORT", "TRINO_HTTP_PORT")
SERVICES = ("minio", "metastore-db", "hive-metastore", "trino-coordinator", "trino-worker")
SCENARIOS = {
    "linked": ("commerce_linked_acceptance", "linked.json", "lakehouse-ops-linked-demo-",
               "three_linked_snapshots_fresh_reference_stack", 1800),
    "owner-exit": ("commerce_owner_exit", "owner-exit.json", "lakehouse-ops-owner-exit-demo-",
                   "owner_kill_after_completed_gold_fresh_reference_stack", 6600),
    "linked-history": ("commerce_linked_acceptance", "linked.json",
                       "lakehouse-ops-linked-history-demo-",
                       "three_linked_snapshots_and_manual_late_customer_maintenance", 2400),
    "profile": ("commerce_pipeline_profile", "profile.json", "lakehouse-ops-scale-demo-",
                "two_volume_pipeline_fresh_reference_stack", 3600),
}


def free_ports() -> dict[str, str]:
    # Hold all sockets together so one probe cannot be selected twice.
    with ExitStack() as stack:
        ports = {}
        for name in PORT_NAMES:
            probe = stack.enter_context(socket.socket())
            probe.bind(("127.0.0.1", 0))
            ports[name] = str(probe.getsockname()[1])
        return ports


def release_demo_network(project: str, root: Path, env: dict[str, str], run: Any) -> dict[str, str]:
    """Release only this demo's empty network; retained volumes stay untouched."""
    name = f"{project}_default"
    options = dict(cwd=root, env=dict(env), capture_output=True, text=True,
                   timeout=30, check=False)
    inspection = run(["docker", "network", "inspect", name], **options)
    if inspection.returncode:
        if f"network {name} not found" in inspection.stderr.lower():
            return {"network": name, "status": "absent"}
        raise RuntimeError(f"demo network inspection failed: {inspection.stderr}")
    networks = json.loads(inspection.stdout)
    if not isinstance(networks, list) or len(networks) != 1 or not isinstance(networks[0], dict):
        raise RuntimeError("unexpected demo network inspection response")
    network = networks[0]
    if (network.get("Name") != name
            or (network.get("Labels") or {}).get("com.docker.compose.project") != project):
        raise RuntimeError("demo network ownership differs; network retained")
    if network.get("Containers") != {}:
        raise RuntimeError("demo network has attached containers; network retained")
    removed = run(["docker", "network", "rm", name], **options)
    if removed.returncode:
        raise RuntimeError(f"demo network release failed: {removed.stderr}")
    return {"network": name, "status": "released"}


def demo(root: Path, output: Path, *, run: Any = subprocess.run,
         scenario: str = "linked", orders: tuple[int, int] | None = None) -> dict[str, Any]:
    if scenario not in SCENARIOS:
        raise ValueError("unsupported reference-stack scenario")
    module, retained, prefix, scope, timeout = SCENARIOS[scenario]
    if scenario == "profile":
        orders = (100, 1000) if orders is None else orders
        if len(orders) != 2 or not 100 <= orders[0] <= orders[1] <= 100_000:
            raise ValueError("requires two ascending or equal order volumes between 100 and 100000")
        if orders[0] == orders[1]:
            scope = "same_volume_fresh_then_warm_reference_stack"
    elif orders is not None:
        raise ValueError("order volumes are only supported for the profile scenario")
    root, output = root.resolve(), output.resolve()
    if not (root / f"src/lakehouse_ops/{module}.py").is_file():
        raise ValueError("requires the lakehouse-ops-platform checkout root")
    if scenario == "profile" and not (root / "compose.spark-memory.yaml").is_file():
        raise ValueError("requires the Spark memory Compose overlay")
    if output.exists() and any(output.iterdir()):
        raise ValueError("requires an empty output directory")
    output.mkdir(parents=True, exist_ok=True)
    project = f"{prefix}{uuid.uuid4().hex}"
    env = dict(os.environ)
    env.pop("AWS_SESSION_TOKEN", None)
    env.pop("COMPOSE_PROFILES", None)
    env.update(free_ports(), COMPOSE_PROJECT_NAME=project,
               COMPOSE_FILE=str(root / "compose.yaml"), COMMERCE_STAGE_TIMEOUT_SECONDS="300",
               MINIO_INGEST_USER="lakeops-ingest", MINIO_INGEST_PASSWORD="ingest-development-only",
               AWS_ACCESS_KEY_ID="lakeops-ingest", AWS_SECRET_ACCESS_KEY="ingest-development-only")
    if scenario == "profile":
        env["COMPOSE_PATH_SEPARATOR"] = os.pathsep
        env["COMPOSE_FILE"] = os.pathsep.join(str(root / name) for name in
                                             ("compose.yaml", "compose.spark-memory.yaml"))
    compose = ["docker", "compose", "--project-name", project, "--profile", "catalog",
               "--profile", "compute", "--profile", "query"]
    report: dict[str, Any] = {"status": "failed", "project": project, "steps": [],
                              "ports": {name: env[name] for name in PORT_NAMES},
                              "scope": scope}
    started = False

    def execute(name: str, command: list[str], timeout: int) -> None:
        step: dict[str, Any] = {"name": name, "status": "failed", "command": command}
        report["steps"].append(step)
        clock = time.perf_counter()
        try:
            with (output / f"{name}.log").open("wb") as log:
                result = run(command, cwd=root, env=dict(env), stdout=log,
                             stderr=subprocess.STDOUT, timeout=timeout, check=False)
            step["returncode"] = result.returncode
            if result.returncode:
                raise RuntimeError(f"{name} exited with {result.returncode}")
            step["status"] = "ready"
        finally:
            step["elapsed_seconds"] = round(time.perf_counter() - clock, 6)

    try:
        execute("docker", ["docker", "info", "--format", "{{.ServerVersion}}"], 30)
        execute("spark-image", ["docker", "image", "inspect",
                                "lakehouse-ops/spark-iceberg:3.5.9-1.11.0"], 30)
        execute("config", [*compose, "config", "--quiet"], 30)
        started = True  # Even partially failed startup needs scoped shutdown.
        execute("start", [*compose, "up", "-d", "--wait", "--wait-timeout", "180",
                          "--no-build", "--pull", "never", *SERVICES], 240)
        execute(scenario, [sys.executable, "-m", f"lakehouse_ops.{module}",
                           "--output", str(output / "acceptance"), "--s3-endpoint-url",
                           f"http://localhost:{env['MINIO_API_PORT']}", "--server",
                           f"http://localhost:{env['TRINO_HTTP_PORT']}"] +
                (["--late-customer-history"] if scenario == "linked-history" else []) +
                (["--orders", *(str(value) for value in orders)] if orders else []), timeout)
        acceptance = json.loads((output / f"acceptance/{retained}").read_text())
        if acceptance.get("status") != "ready":
            raise RuntimeError(f"{scenario} acceptance did not retain a ready report")
        report["acceptance"] = acceptance
        report["status"] = "ready"
    except (OSError, RuntimeError, subprocess.TimeoutExpired, ValueError) as error:
        report.update(status="failed", error_type=type(error).__name__, error=str(error))
    finally:
        if started:
            try:
                # Remove this uniquely owned stack, including completed init containers.
                # Keep named warehouse volumes: never use --volumes or --rmi.
                execute("stop", [*compose, "down"], 120)
                report["network_cleanup"] = release_demo_network(project, root, env, run)
            except (OSError, RuntimeError, subprocess.TimeoutExpired, ValueError) as error:
                report.update(status="failed", cleanup_error=str(error))
        (output / "demo.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        report = demo(Path.cwd(), args.output)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())
