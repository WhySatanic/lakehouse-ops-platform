from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def validate(report: dict[str, Any]) -> list[str]:
    policy = report.get("policy", {})
    groups = policy.get("groups", {})
    assignments = report.get("assignments", {})
    queue = report.get("queue", {})
    checks = {
        "schema_version": report.get("schema_version") == "1.0",
        "status": report.get("status") == "succeeded",
        "root_policy": policy.get("root") == "global"
        and policy.get("root_hard_concurrency") == 4,
        "ingestion_policy": groups.get("ingestion")
        == {"hard_concurrency": 1, "max_queued": 4},
        "bi_policy": groups.get("bi")
        == {"hard_concurrency": 2, "max_queued": 10},
        "adhoc_policy": groups.get("adhoc")
        == {"hard_concurrency": 1, "max_queued": 3},
        "assignments": assignments
        == {
            "ingestion": "global.ingestion",
            "bi": "global.bi",
            "adhoc": "global.adhoc",
        },
        "queue_group": queue.get("group") == "global.adhoc",
        "running_state": queue.get("running_state") in {"RUNNING", "FINISHING"},
        "queued_state": queue.get("queued_state") == "QUEUED",
        "cleanup": report.get("cleanup")
        == {"queries_submitted": 4, "queries_cancelled": 4},
        "continuity": report.get("continuity") == {"silver_rows": 2},
    }
    return [name for name, passed in checks.items() if not passed]


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_trino_resource_groups.py REPORT")
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    failed = validate(report)
    if failed:
        raise SystemExit(f"Trino resource group evidence failed: {', '.join(failed)}")
    print(
        json.dumps(
            {
                "status": "ready",
                "groups": 3,
                "queued_queries": 1,
                "cancelled_queries": 4,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
