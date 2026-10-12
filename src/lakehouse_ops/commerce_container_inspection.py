"""Observe journal-recorded Docker stages without cleanup or owner-liveness claims."""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from collections import Counter
from contextlib import closing
from pathlib import Path
from typing import Any

from lakehouse_ops.commerce_run_journal import CommerceJournalError


def inspect_containers(
    path: Path, *, project: str, limit: int = 10, run: Any = subprocess.run,
) -> dict[str, Any]:
    if not project or type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("requires an explicit project and limit between 1 and 100")
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True,
                                     timeout=5)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            rows = connection.execute(
                "SELECT run_id,batch_id,manifest_sha256,service,container_name,project_hint "
                "FROM commerce_container_events WHERE project_hint=? AND event='launch_intent' "
                "ORDER BY sequence DESC LIMIT ?", (project, limit + 1),
            ).fetchall()
    except sqlite3.Error as error:
        raise CommerceJournalError(f"cannot inspect container journal: {path}: {error}") from error
    containers = []
    for row in rows[:limit]:
        item = dict(row)
        name = item["container_name"]
        item["observation"] = "inspect_failed"
        try:
            result = run(["docker", "container", "inspect", "--", name], capture_output=True,
                         text=True, check=False, timeout=5)
            if result.returncode:
                # Only Docker's exact missing-container response proves absence.
                missing = {f"error: no such container: {name}".lower(),
                           f"error response from daemon: no such container: {name}".lower()}
                if result.stderr.strip().lower() in missing:
                    item["observation"] = "absent"
                else:
                    item["error_type"] = "inspect_exit"
            else:
                data = json.loads(result.stdout)
                if not isinstance(data, list) or len(data) != 1:
                    raise ValueError("requires one inspected container")
                container = data[0]
                labels = container["Config"]["Labels"] or {}
                wanted = {
                    "lakehouse.ops.run_id": item["run_id"],
                    "lakehouse.ops.batch_id": item["batch_id"],
                    "lakehouse.ops.manifest_sha256": item["manifest_sha256"],
                    "com.docker.compose.project": project,
                    "com.docker.compose.service": item["service"],
                }
                if (container["Name"] != f"/{name}"
                        or any(labels.get(key) != value for key, value in wanted.items())):
                    item["observation"] = "ownership_mismatch"
                else:
                    state = container["State"]["Status"]
                    if state not in {"running", "exited", "created", "paused", "restarting",
                                     "dead", "removing"}:
                        raise ValueError("unsupported Docker state")
                    item.update(observation=state, container_id=container["Id"])
        except (OSError, subprocess.TimeoutExpired, ValueError, KeyError, TypeError,
                AttributeError) as error:
            item["error_type"] = type(error).__name__
        containers.append(item)
    counts = dict(Counter(item["observation"] for item in containers))
    return {
        "status": "incomplete" if counts.get("inspect_failed") else "observed",
        "scope": "recorded_containers_not_owner_liveness_or_recovery",
        "journal": str(path.resolve()), "project": project, "limit": limit,
        "has_more": len(rows) > limit, "counts": counts, "containers": containers,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args(argv)
    try:
        report = inspect_containers(args.journal, project=args.project, limit=args.limit)
    except (OSError, ValueError) as error:
        print(f"commerce container inspection failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report, sort_keys=True))
    return 2 if report["status"] == "incomplete" else 0


if __name__ == "__main__":
    raise SystemExit(main())
