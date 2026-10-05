from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES

CHECKER_PATH = Path(__file__).parent / "integration" / "check_commerce_cycle.py"
SPEC = importlib.util.spec_from_file_location("check_commerce_cycle", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


def _evidence(tmp_path: Path) -> tuple[Path, Path, Path]:
    batch_id = "a" * 16
    verification = {"status": "ready", "batch_id": batch_id, "orders": 6}
    reference = {"batch_id": batch_id, "verification": verification}
    pending = {
        "status": "ready", "batch_id": batch_id,
        "completed_stages": list(COMMERCE_STAGES),
        "verification": verification, "checkpoint": {"created": True},
    }
    idle = {"status": "idle", "batch_id": None, "completed_stages": []}
    notification = {
        "status": "ready", "notification": "accepted", "instance": "commerce-ci",
        "alerts_sent": 2, "source": {"status": "ready", "latest_batch_id": batch_id},
        "backlog": {"status": "ready", "pending_batches": 0},
    }
    paths = tuple(tmp_path / name for name in ("pending.jsonl", "idle.jsonl", "reference.json"))
    paths[0].write_text("\n".join(json.dumps(row) for row in (pending, notification)) + "\n")
    paths[1].write_text("\n".join(json.dumps(row) for row in (idle, notification)) + "\n")
    paths[2].write_text(json.dumps(reference))
    return paths


def test_accepts_pending_then_idle_cycle_evidence(tmp_path: Path) -> None:
    report = CHECKER.check_cycle_evidence(*_evidence(tmp_path))
    assert report == {
        "status": "ready", "batch_id": "a" * 16,
        "completed_stages": len(COMMERCE_STAGES),
        "checkpoint_created": True, "replay_status": "idle", "alerts_accepted": 4,
    }


@pytest.mark.parametrize("failure", ["missing_stage", "stale_alert", "missing_line"])
def test_rejects_incomplete_cycle_evidence(tmp_path: Path, failure: str) -> None:
    pending_path, idle_path, reference_path = _evidence(tmp_path)
    if failure == "missing_line":
        idle_path.write_text(idle_path.read_text().splitlines()[0] + "\n")
    else:
        reports: list[dict[str, Any]] = [
            json.loads(line) for line in pending_path.read_text().splitlines()
        ]
        if failure == "missing_stage":
            reports[0]["completed_stages"].pop()
        else:
            reports[1]["backlog"]["status"] = "stale"
        pending_path.write_text("\n".join(json.dumps(report) for report in reports) + "\n")
    with pytest.raises(ValueError):
        CHECKER.check_cycle_evidence(pending_path, idle_path, reference_path)
