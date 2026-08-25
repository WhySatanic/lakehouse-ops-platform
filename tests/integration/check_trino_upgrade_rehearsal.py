from __future__ import annotations

import json
import sys
from pathlib import Path

from lakehouse_ops.trino_upgrade import (
    UpgradeRehearsalError,
    load_upgrade_plan,
    validate_upgrade_report,
)


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "usage: check_trino_upgrade_rehearsal.py REPORT.json PLAN.json"
        )
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    plan = load_upgrade_plan(Path(sys.argv[2]))
    try:
        validate_upgrade_report(report, plan)
    except UpgradeRehearsalError as error:
        raise SystemExit(f"Trino upgrade rehearsal evidence failed: {error}") from error
    print(
        json.dumps(
            {
                "status": "ready",
                "source_version": plan["source"]["version"],
                "target_version": plan["target"]["version"],
                "phases": [phase["phase"] for phase in report["phases"]],
                "data_fingerprint": report["phases"][0]["data_fingerprint"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
