"""Bounded late-effective customer observation through S3 and production Spark jobs."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from lakehouse_ops.commerce_gold_gate import check_commerce_gold
from lakehouse_ops.commerce_linked_acceptance import verify_snapshot
from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES, CURRENT_STAGE, run_commerce_batch
from lakehouse_ops.commerce_recovery import audit_tables, verify_recovery_audit
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
    """Derive the late observation used by maintenance and full-batch continuation."""
    return _generate_observation(initial, output, "2026-02-01T12:00:00Z", "Alice Corrected")


def generate_following_observation(latest: Path, output: Path) -> dict[str, str]:
    """A new full snapshot with unchanged customer and order state after correction."""
    return _generate_observation(latest, output, "2026-02-04T00:00:00Z", None)


def _generate_observation(
    initial: Path, output: Path, batch_at: str, customer_name: str | None,
) -> dict[str, str]:
    manifest = json.loads((initial / "manifest.json").read_text(encoding="utf-8"))
    manifest["config"]["batch_at"] = batch_at
    bodies = {}
    for table, item in manifest["tables"].items():
        rows = [json.loads(line) for line in (initial / item["file"]).read_text(
            encoding="utf-8",
        ).splitlines()]
        if table == "customers" and customer_name is not None:
            rows[0]["full_name"] = customer_name
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


def exercise_late_batch_completion(
    planner, query: Callable[[str], list[dict[str, Any]]], run_stage: Callable[..., None],
    batch: dict[str, str], expected: dict[str, Any], output: Path, report: dict[str, Any],
) -> None:
    """Fail after late batch gold commits, then retry through the ordinary runner."""
    class LateBatchDrillFailure(RuntimeError):
        pass

    sample: dict[str, Any] = {
        "status": "running", "batch_id": batch["batch_id"],
        "failure_boundary": "after_batch_gold_before_current",
    }
    report["late_batch_recovery"] = sample
    checkpoint_path = output / "checkpoint.json"
    corrected = {**expected, "customer_history": CORRECTED_HISTORY}
    latest = expected["batches"][-1]["batch_id"]

    def snapshots():
        return {table: query('SELECT count(*) AS snapshots FROM lakehouse.gold.'
                             f'"{table}$snapshots"')[0]["snapshots"]
                for table in ("commerce_daily", "commerce_current_daily", "dim_customers_scd2")}

    def verify_late_gold():
        gold = check_commerce_gold(query, batch["batch_id"])
        if (gold.days != 1 or gold.orders != 1
                or gold.captured_revenue_cents != expected["snapshot_revenue_cents"][0]):
            raise ValueError("late batch gold differs from the initial snapshot literal oracle")
        return gold.as_dict()

    try:
        checkpoint = checkpoint_path.read_bytes()
        sample["checkpoint_before_sha256"] = hashlib.sha256(checkpoint).hexdigest()
        before = snapshots()
        sample["snapshots_before"] = before
        failed = CommerceRunJournal(output / "run-journal.sqlite3")
        sample["failed_run_id"] = failed.run_id

        def execute(service, selected, journal):
            if selected["batch_id"] != batch["batch_id"]:
                raise ValueError("runner did not select the late full snapshot")
            if selected.get("customer_history_mode") != "rebuild":
                raise ValueError("late full snapshot did not select retained-history rebuild")
            run_stage(service, selected, journal=journal)

        def fail_stage(service, selected):
            execute(service, selected, failed)
            if service == COMMERCE_STAGES[-1]:
                raise LateBatchDrillFailure(
                    "injected after committed batch gold, before current gold",
                )

        started = time.perf_counter()
        try:
            run_commerce_batch(planner, query, run_stage=fail_stage, journal=failed)
        except LateBatchDrillFailure:
            sample["failed_attempt_seconds"] = time.perf_counter() - started
        else:
            raise ValueError("late full snapshot did not reach the injected failure boundary")
        checkpoint_after_failure = checkpoint_path.read_bytes()
        if checkpoint_after_failure != checkpoint:
            raise ValueError("failed late full snapshot advanced checkpoint")
        sample["checkpoint_after_failure_sha256"] = hashlib.sha256(
            checkpoint_after_failure,
        ).hexdigest()
        sample["checkpoint_after_failure"] = "unchanged"
        verify_snapshot(query, corrected, 2, latest)
        sample["batch_gold_after_failure"] = verify_late_gold()
        after_failure = snapshots()
        if after_failure != {**before, "commerce_daily": before["commerce_daily"] + 1}:
            raise ValueError(
                "late batch failure changed history/current or missed batch gold commit",
            )
        sample["snapshots_after_failure"] = after_failure
        sample["published_after_failure"] = audit_tables(query)
        retry = CommerceRunJournal(output / "run-journal.sqlite3")
        sample["retry_run_id"] = retry.run_id
        oracle = {}

        def retry_stage(service, selected):
            nonlocal oracle
            execute(service, selected, retry)
            if service == CURRENT_STAGE:
                oracle = verify_snapshot(query, corrected, 2, latest)
                if verify_late_gold() != sample["batch_gold_after_failure"]:
                    raise ValueError("late batch retry changed its batch gold result")
                sample["snapshots_before_retry_checkpoint"] = snapshots()
                after_retry = sample["snapshots_before_retry_checkpoint"]
                if any(after_retry[table] != after_failure[table] for table in (
                    "commerce_current_daily", "dim_customers_scd2",
                )):
                    raise ValueError("late batch retry changed current/history snapshots")
                sample["published_before_retry_checkpoint"] = audit_tables(query)
                verify_recovery_audit(sample["published_after_failure"],
                                      sample["published_before_retry_checkpoint"])
                if checkpoint_path.read_bytes() != checkpoint:
                    raise ValueError("late batch retry checkpoint preceded verification")

        completed = run_commerce_batch(planner, query, run_stage=retry_stage, journal=retry)
        if (not oracle or completed["status"] != "ready"
                or completed["completed_stages"] != [*COMMERCE_STAGES, CURRENT_STAGE]):
            raise ValueError("late batch retry did not complete every verified stage")
        sample.update(status="ready", oracle=oracle, batch_gold_replay="rows_and_files_stable",
                      retry_seconds=completed["durations_seconds"]["total"])
        report["late_batch_completion"] = completed
    except BaseException as error:
        sample.update(status="failed", error_type=type(error).__name__)
        report["status"] = "failed"
        raise


def exercise_following_batch(
    planner, query: Callable[[str], list[dict[str, Any]]], run_stage: Callable[..., None],
    batch: dict[str, str], expected: dict[str, Any], output: Path, report: dict[str, Any],
) -> None:
    """Prove the next ordinary full snapshot preserves the corrected history."""
    snapshots_sql = ('SELECT count(*) AS snapshots FROM lakehouse.gold.'
                     '"dim_customers_scd2$snapshots"')
    history_before = query(snapshots_sql)
    journal = CommerceRunJournal(output / "run-journal.sqlite3")
    oracle = {}

    def stage(service, selected):
        nonlocal oracle
        if selected["batch_id"] != batch["batch_id"]:
            raise ValueError("runner did not select the next ordinary full snapshot")
        if selected.get("customer_history_mode") != "forward":
            raise ValueError("next ordinary full snapshot did not use forward history")
        run_stage(service, selected, journal=journal)
        if service == CURRENT_STAGE:
            oracle = verify_snapshot(query, {**expected, "customer_history": CORRECTED_HISTORY},
                                     2, batch["batch_id"])
            if query(snapshots_sql) != history_before:
                raise ValueError("unchanged following customer created a history snapshot")

    completed = run_commerce_batch(planner, query, run_stage=stage, journal=journal)
    if not oracle:
        raise ValueError("next ordinary full snapshot did not publish current gold")
    report["next_batch_after_correction"] = {**completed, "oracle": oracle}
    def idle_stage(*_):
        raise ValueError("idle ran compute")

    report["idle_after_next_batch"] = run_commerce_batch(planner, query, run_stage=idle_stage)
    if report["idle_after_next_batch"]["status"] != "idle":
        raise ValueError("next ordinary full snapshot left a pending batch")


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
