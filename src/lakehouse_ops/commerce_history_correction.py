"""Bounded late-effective customer observation through S3 and production Spark jobs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from lakehouse_ops.commerce_linked_acceptance import verify_snapshot
from lakehouse_ops.commerce_run_journal import CommerceRunJournal

HISTORY = "lakehouse.gold.dim_customers_scd2"
HISTORY_STAGE = "spark-commerce-customer-scd2"
CORRECTED_HISTORY = [
    {"full_name": "Alice", "valid_from": "2026-02-01T00:00:00Z",
     "valid_to": "2026-02-01T12:00:00Z"},
    {"full_name": "Alice Corrected", "valid_from": "2026-02-01T12:00:00Z",
     "valid_to": "2026-02-02T00:00:00Z"},
    {"full_name": "Alice Updated", "valid_from": "2026-02-02T00:00:00Z", "valid_to": None},
]


def generate_late_observation(initial: Path, output: Path) -> dict[str, str]:
    """Derive a complete synthetic fixture; only its customer rows will be processed."""
    manifest = json.loads((initial / "manifest.json").read_text(encoding="utf-8"))
    manifest["config"]["batch_at"] = "2026-02-01T12:00:00Z"
    bodies = {}
    for table, item in manifest["tables"].items():
        rows = [json.loads(line) for line in (initial / item["file"]).read_text(
            encoding="utf-8",
        ).splitlines()]
        if table == "customers":
            rows[0]["full_name"] = "Alice Corrected"
        bodies[item["file"]] = b"".join(
            (json.dumps(row, sort_keys=True) + "\n").encode() for row in rows
        )
        item["sha256"] = hashlib.sha256(bodies[item["file"]]).hexdigest()
    manifest["batch_id"] = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()
                                         ).hexdigest()[:16]
    body = (json.dumps(manifest, sort_keys=True) + "\n").encode()
    output.mkdir(parents=True, exist_ok=False)
    for name, content in bodies.items():
        (output / name).write_bytes(content)
    (output / "manifest.json").write_bytes(body)
    return {"batch_id": manifest["batch_id"], "manifest_sha256": hashlib.sha256(body).hexdigest(),
            "batch_at": manifest["config"]["batch_at"]}


def exercise_history_correction(
    query: Callable[[str], list[dict[str, Any]]], run_stage: Callable[..., None],
    batch: dict[str, str], expected: dict[str, Any], output: Path, report: dict[str, Any],
) -> None:
    """Maintenance drill, not a fourth checkpoint or an automatically planned backfill."""
    checkpoint = (output / "checkpoint.json").read_bytes()
    latest = expected["batches"][-1]["batch_id"]
    verify_snapshot(query, expected, 2, latest)
    snapshots_sql = f'SELECT count(*) AS snapshots FROM {HISTORY.rsplit(".", 1)[0]}.' + (
        '"dim_customers_scd2$snapshots"'
    )
    current_sql = ('SELECT count(*) AS snapshots FROM lakehouse.gold.'
                   '"commerce_current_daily$snapshots"')
    history_before = query(snapshots_sql)[0]["snapshots"]
    current_before = query(current_sql)
    corrected = {**expected, "customer_history": CORRECTED_HISTORY}
    sample: dict[str, Any] = {"status": "running", "batch_id": batch["batch_id"], "runs": []}
    report["late_customer_correction"] = sample
    for replay in (False, True):
        journal = CommerceRunJournal(output / "run-journal.sqlite3")
        sample["runs"].append(journal.run_id)
        journal.record("maintenance", "selected", batch=batch)
        try:
            stages = [] if replay else ["bronze-input-sync", "spark-commerce-bronze",
                                       "spark-commerce-customer-silver"]
            for service in [*stages, HISTORY_STAGE]:
                journal.record(service, "started")
                run_stage(service, batch, journal=journal,
                          history_mode="rebuild" if service == HISTORY_STAGE else "forward")
                journal.record(service, "completed")
            journal.record("verification", "started")
            oracle = verify_snapshot(query, corrected, 2, latest)
            if query(snapshots_sql)[0]["snapshots"] != history_before + 1:
                raise ValueError("history correction must add exactly one snapshot across replay")
            if query(current_sql) != current_before:
                raise ValueError("customer correction changed current revenue snapshots")
            if (output / "checkpoint.json").read_bytes() != checkpoint:
                raise ValueError("history maintenance changed processing checkpoint")
            journal.record("verification", "completed")
        except BaseException as error:
            sample.update(status="failed", error_type=type(error).__name__)
            journal.record(journal.phase, "failed", error_type=type(error).__name__)
            raise
        journal.record("run", "ready")
    sample.update(status="ready", oracle=oracle, history_snapshots_added=1,
                  replay="snapshot_stable", checkpoint="unchanged",
                  scope="late_effective_customer_observation_manual_maintenance")
