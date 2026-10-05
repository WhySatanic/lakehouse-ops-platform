from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES


def _reports(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) != 2:
        raise ValueError(f"expected exactly two commerce cycle reports: {path}")
    reports = [json.loads(line) for line in lines]
    if not all(isinstance(report, dict) for report in reports):
        raise ValueError(f"commerce cycle reports must be objects: {path}")
    return reports[0], reports[1]


def check_cycle_evidence(
    pending_path: Path, idle_path: Path, reference_path: Path,
) -> dict[str, Any]:
    pending, pending_alert = _reports(pending_path)
    idle, idle_alert = _reports(idle_path)
    reference = json.loads(reference_path.read_text(encoding="utf-8"))
    batch_id = reference["batch_id"]
    if not (
        pending.get("status") == "ready"
        and pending.get("batch_id") == batch_id
        and pending.get("completed_stages") == list(COMMERCE_STAGES)
        and pending.get("verification") == reference["verification"]
        and pending.get("checkpoint", {}).get("created") is True
    ):
        raise ValueError("scheduled cycle did not verify and checkpoint the pending batch")
    if not (
        idle.get("status") == "idle"
        and idle.get("batch_id") is None
        and idle.get("completed_stages") == []
    ):
        raise ValueError("scheduled replay did not remain idle")
    for report in (pending_alert, idle_alert):
        if not (
            report.get("status") == "ready"
            and report.get("notification") == "accepted"
            and report.get("instance") == "commerce-ci"
            and report.get("alerts_sent") == 2
            and report.get("source", {}).get("status") == "ready"
            and report.get("source", {}).get("latest_batch_id") == batch_id
            and report.get("backlog", {}).get("status") == "ready"
            and report.get("backlog", {}).get("pending_batches") == 0
        ):
            raise ValueError("scheduled cycle did not submit healthy freshness observations")
    return {
        "status": "ready", "batch_id": batch_id,
        "completed_stages": len(COMMERCE_STAGES),
        "checkpoint_created": True, "replay_status": "idle", "alerts_accepted": 4,
    }


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("usage: check_commerce_cycle.py PENDING.jsonl IDLE.jsonl REFERENCE.json")
    print(json.dumps(check_cycle_evidence(*(Path(arg) for arg in sys.argv[1:])), sort_keys=True))


if __name__ == "__main__":
    main()
