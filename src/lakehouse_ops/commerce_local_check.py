"""One-command local quality gates and two bounded, isolated Docker smoke tests."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from lakehouse_ops.local_quality import CHECKS


def check(root: Path, output: Path, *, run: Any = subprocess.run) -> dict[str, Any]:
    root, output = root.resolve(), output.resolve()
    if not (root / "src/lakehouse_ops/commerce_pipeline.py").is_file():
        raise ValueError("run from the lakehouse-ops-platform checkout root")
    if output.exists() and any(output.iterdir()):
        raise ValueError("requires an empty output directory")
    output.mkdir(parents=True, exist_ok=True)
    env = {key: value for key, value in os.environ.items()
           if not key.startswith("LAKEOPS_RUN_") and key != "PYTEST_ADDOPTS"}
    env["COMPOSE_PROJECT_NAME"] = f"lakehouse-ops-local-check-{uuid.uuid4().hex}"
    python = [sys.executable, "-m"]
    steps = [
        ("docker", ["docker", "info", "--format", "{{.ServerVersion}}"], 30),
        ("spark-image", ["docker", "image", "inspect",
                         "lakehouse-ops/spark-iceberg:3.5.9-1.11.0"], 30),
        *[
            ({"tests": "quality", "trino-policy": "access-policy"}.get(name, name),
             [*command, *([f"--junitxml={output / 'quality.xml'}"] if name == "tests" else [])],
             600 if name == "tests" else 120)
            for name, command in CHECKS
        ],
        ("docker-smoke", [*python, "pytest",
                          "tests/integration/test_commerce_stage_timeout.py",
                          "tests/integration/test_spark_memory.py", "-q", "-s", "-o",
                          "addopts=", f"--junitxml={output / 'docker-smoke.xml'}"], 300),
    ]
    report: dict[str, Any] = {"status": "failed", "scope": "local_quality_and_docker_smoke",
                              "steps": [], "project": env["COMPOSE_PROJECT_NAME"]}
    try:
        for name, command, timeout in steps:
            step: dict[str, Any] = {"name": name, "command": command, "status": "failed"}
            report["steps"].append(step)
            step_env = dict(env)
            if name == "docker-smoke":
                step_env.update(LAKEOPS_RUN_DOCKER_TIMEOUT_SMOKE="1",
                                LAKEOPS_RUN_SPARK_MEMORY_SMOKE="1")
            started = time.perf_counter()
            try:
                with (output / f"{name}.log").open("wb") as log:
                    result = run(command, cwd=root, env=step_env, stdout=log,
                                 stderr=subprocess.STDOUT, timeout=timeout, check=False)
                step["returncode"] = result.returncode
                if result.returncode:
                    raise RuntimeError(f"{name} exited with {result.returncode}")
                if name == "docker-smoke":
                    cases = ET.parse(output / "docker-smoke.xml").findall(".//testcase")
                    required = {
                        ("tests.integration.test_commerce_stage_timeout",
                         "test_real_compose_timeout_removes_owned_container"),
                        ("tests.integration.test_spark_memory",
                         "test_real_spark_session_emits_container_memory"),
                    }
                    executed = {(case.get("classname"),
                                 case.get("name", "").split("[", 1)[0]) for case in cases}
                    if not required <= executed or any(
                        case.find(tag) is not None
                        for case in cases for tag in ("skipped", "failure", "error")
                    ):
                        raise RuntimeError("requires executed, passing timeout and memory smokes "
                                           "and no skipped or failed collected smoke tests")
                step["status"] = "ready"
            finally:
                step["elapsed_seconds"] = round(time.perf_counter() - started, 6)
        report["status"] = "ready"
    except (OSError, subprocess.TimeoutExpired, RuntimeError, ET.ParseError) as error:
        report["error_type"] = type(error).__name__
        report["error"] = str(error)
    finally:
        (output / "check.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        report = check(Path.cwd(), args.output)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())
