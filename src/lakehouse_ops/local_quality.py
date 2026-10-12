"""Run the repository's local quality gates without starting containers."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

CHECKS = (
    ("lock", ["uv", "lock", "--check"]),
    ("lint", ["uv", "run", "--frozen", "ruff", "check", "."]),
    ("contract", ["uv", "run", "--frozen", "lakeops", "verify-control-plane-contract",
                  "--contract", "config/control-plane/contract.json"]),
    ("trino-policy", ["uv", "run", "--frozen", "lakeops", "render-trino-access-policy",
                      "--model", "config/access/role-policy.json",
                      "--output", "infra/trino/access-control-rules.json", "--check"]),
    ("image-lock", ["uv", "run", "--frozen", "lakeops", "verify-image-lock"]),
    ("compose", ["docker", "compose", "--profile", "catalog", "--profile", "compute",
                 "--profile", "query", "--profile", "security", "--profile", "observability",
                 "--profile", "serving", "config", "--quiet"]),
    ("tests", ["uv", "run", "--frozen", "pytest", "-q"]),
)


def run_quality(
    root: Path, *, timeout_seconds: int = 300,
    run: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
    clock: Callable[[], float] = time.perf_counter,
) -> dict[str, Any]:
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600:
        raise ValueError("timeout_seconds must be between 1 and 3600")
    root = root.resolve()
    if not (root / "pyproject.toml").is_file() or not (root / "compose.yaml").is_file():
        raise ValueError("root must contain pyproject.toml and compose.yaml")
    results = []
    for name, command in CHECKS:
        started = clock()
        reason = None
        code = None
        print(f"Checking {name}", file=sys.stderr)
        try:
            code = run(command, cwd=root, check=False, stdout=sys.stderr,
                       timeout=timeout_seconds).returncode
            if code != 0:
                reason = "exit_status"
        except subprocess.TimeoutExpired:
            reason = "timeout"
        except OSError:
            reason = "could_not_start"
        results.append({
            "name": name, "status": "passed" if reason is None else "failed",
            "return_code": code, "reason": reason, "elapsed_seconds": clock() - started,
        })
        if reason is not None:
            break
    return {"status": "ready" if reason is None else "failed", "root": str(root),
            "checks": results, "scope": "local_quality_not_integration"}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--timeout-seconds", type=int, default=300)
    args = parser.parse_args(argv)
    try:
        report = run_quality(args.root, timeout_seconds=args.timeout_seconds)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
