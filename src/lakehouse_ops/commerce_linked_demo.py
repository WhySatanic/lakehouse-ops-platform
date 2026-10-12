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


def free_ports() -> dict[str, str]:
    # Hold all sockets together so one probe cannot be selected twice.
    with ExitStack() as stack:
        ports = {}
        for name in PORT_NAMES:
            probe = stack.enter_context(socket.socket())
            probe.bind(("127.0.0.1", 0))
            ports[name] = str(probe.getsockname()[1])
        return ports


def demo(root: Path, output: Path, *, run: Any = subprocess.run) -> dict[str, Any]:
    root, output = root.resolve(), output.resolve()
    if not (root / "src/lakehouse_ops/commerce_linked_acceptance.py").is_file():
        raise ValueError("requires the lakehouse-ops-platform checkout root")
    if output.exists() and any(output.iterdir()):
        raise ValueError("requires an empty output directory")
    output.mkdir(parents=True, exist_ok=True)
    project = f"lakehouse-ops-linked-demo-{uuid.uuid4().hex}"
    env = dict(os.environ)
    env.pop("AWS_SESSION_TOKEN", None)
    env.pop("COMPOSE_PROFILES", None)
    env.update(free_ports(), COMPOSE_PROJECT_NAME=project,
               COMPOSE_FILE=str(root / "compose.yaml"), COMMERCE_STAGE_TIMEOUT_SECONDS="300",
               MINIO_INGEST_USER="lakeops-ingest", MINIO_INGEST_PASSWORD="ingest-development-only",
               AWS_ACCESS_KEY_ID="lakeops-ingest", AWS_SECRET_ACCESS_KEY="ingest-development-only")
    compose = ["docker", "compose", "--project-name", project, "--profile", "catalog",
               "--profile", "compute", "--profile", "query"]
    report: dict[str, Any] = {"status": "failed", "project": project, "steps": [],
                              "ports": {name: env[name] for name in PORT_NAMES},
                              "scope": "three_linked_snapshots_fresh_reference_stack"}
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
        execute("linked", [sys.executable, "-m", "lakehouse_ops.commerce_linked_acceptance",
                           "--output", str(output / "acceptance"), "--s3-endpoint-url",
                           f"http://localhost:{env['MINIO_API_PORT']}", "--server",
                           f"http://localhost:{env['TRINO_HTTP_PORT']}"], 1800)
        linked = json.loads((output / "acceptance/linked.json").read_text())
        if linked.get("status") != "ready":
            raise RuntimeError("linked acceptance did not retain a ready report")
        report["acceptance"] = linked
        report["status"] = "ready"
    except (OSError, RuntimeError, subprocess.TimeoutExpired, ValueError) as error:
        report.update(status="failed", error_type=type(error).__name__, error=str(error))
    finally:
        if started:
            try:
                execute("stop", [*compose, "stop", *SERVICES], 120)
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
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
