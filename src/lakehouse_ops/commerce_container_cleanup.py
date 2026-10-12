"""Explicit single-container cleanup under the same checkout's commerce runner lock."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path
from typing import Any

from lakehouse_ops.commerce_container_inspection import inspect_containers
from lakehouse_ops.commerce_runner_lock import CommerceRunnerBusyError, commerce_runner_lock


def cleanup_container(
    path: Path, *, project: str, run_id: str, container_name: str,
    run: Any = subprocess.run,
) -> dict[str, Any]:
    if (not project.startswith("lakehouse-ops-")
            or re.fullmatch(r"[a-f0-9]{32}", run_id) is None
            or re.fullmatch(r"lakeops-commerce-[a-f0-9]{32}", container_name) is None):
        raise ValueError("requires a scoped project, exact run ID and recorded stage name")
    report = {"status": "refused", "project": project, "run_id": run_id,
              "container_name": container_name, "scope": "explicit_local_single_container_cleanup"}
    try:
        with commerce_runner_lock():
            observed = inspect_containers(path, project=project, limit=2, run=run,
                                          run_id=run_id, container_name=container_name)
            if len(observed["containers"]) != 1 or observed["has_more"]:
                return {**report, "reason": "requires_one_recorded_launch_intent"}
            item = observed["containers"][0]
            if item["observation"] == "absent":
                return {**report, "status": "ready", "action": "already_absent"}
            if item["observation"] not in {"running", "exited", "created", "paused",
                                            "restarting", "dead"}:
                return {**report, "reason": item["observation"]}
            container_id = item["container_id"]
            if (not isinstance(container_id, str)
                    or re.fullmatch(r"[a-f0-9]{64}", container_id) is None):
                return {**report, "reason": "invalid_immutable_container_id"}
            report["container_id"] = container_id
            # Remove by immutable ID, never the reusable name inspected above.
            removed = run(["docker", "container", "rm", "--force", "--", container_id],
                          capture_output=True, text=True, check=False, timeout=30)
            if removed.returncode:
                return {**report, "status": "incomplete", "reason": "remove_failed"}
            confirmed = run(["docker", "container", "inspect", "--", container_id],
                            capture_output=True, text=True, check=False, timeout=5)
            missing = {f"error: no such container: {container_id}",
                       f"error response from daemon: no such container: {container_id}"}
            if not confirmed.returncode or confirmed.stderr.strip().lower() not in missing:
                return {**report, "status": "incomplete", "reason": "absence_unconfirmed"}
            return {**report, "status": "ready", "action": "removed"}
    except CommerceRunnerBusyError:
        return {**report, "reason": "active_local_runner_lock"}
    except (OSError, subprocess.TimeoutExpired, ValueError) as error:
        return {**report, "status": "incomplete", "reason": type(error).__name__}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--confirm-stop", action="store_true",
                        help="Acknowledge stopping this exact stage, possibly during a write")
    args = parser.parse_args(argv)
    if not args.confirm_stop:
        parser.error("requires --confirm-stop; inspect the stage and stop competing writers")
    if not Path("src/lakehouse_ops/commerce_pipeline.py").is_file():
        parser.error("run from the same checkout root used by the commerce runner")
    try:
        report = cleanup_container(args.journal, project=args.project, run_id=args.run_id,
                                   container_name=args.container)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())
