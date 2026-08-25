from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def validate(report: dict[str, Any]) -> list[str]:
    topology = report.get("topology", {})
    shutdown = report.get("shutdown", {})
    continuity = report.get("continuity", {})
    checks = {
        "schema_version": report.get("schema_version") == "1.0",
        "status": report.get("status") == "succeeded",
        "active_nodes_before": topology.get("active_nodes_before") == 3,
        "active_workers_before": topology.get("active_workers_before") == 2,
        "active_nodes_after": topology.get("active_nodes_after") == 2,
        "active_workers_after": topology.get("active_workers_after") == 1,
        "target_node": shutdown.get("target_node_id") == "lakehouse-worker-2",
        "state_before": shutdown.get("state_before") == "ACTIVE",
        "state_after_request": shutdown.get("state_after_request") == "SHUTTING_DOWN",
        "target_unregistered": shutdown.get("target_registered_after") is False,
        "endpoint_stopped": shutdown.get("endpoint_stopped") is True,
        "grace_period": shutdown.get("grace_period_seconds") == 5,
        "silver_rows": continuity.get("silver_rows") == 2,
        "metadata_readable": continuity.get("snapshot_metadata_readable") is True,
    }
    return [name for name, passed in checks.items() if not passed]


def main() -> None:
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    failed = validate(report)
    if failed:
        raise SystemExit(f"Trino worker shutdown evidence failed: {', '.join(failed)}")
    print(
        json.dumps(
            {
                "status": "ready",
                "active_workers_before": 2,
                "active_workers_after": 1,
                "continuity_queries": 2,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
