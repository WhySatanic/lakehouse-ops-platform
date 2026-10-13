from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import httpx

from lakehouse_ops.commerce_gold_gate import (
    CommerceGoldGateError,
    check_commerce_gold_with_retry,
    validate_commerce_gold_retry,
)
from lakehouse_ops.commerce_run_journal import CommerceRunJournal
from lakehouse_ops.ingestion.commerce_batches import CommerceBatchPlanner
from lakehouse_ops.trino import TrinoProtocolError, TrinoQueryError

COMMERCE_STAGES = (
    "bronze-input-sync",
    "spark-commerce-bronze",
    "spark-commerce-payment-silver",
    "spark-commerce-order-silver",
    "spark-commerce-product-silver",
    "spark-commerce-customer-silver",
    "spark-commerce-customer-scd2",
    "spark-commerce-daily-gold",
)


class CommercePipelineError(RuntimeError):
    pass


def commerce_query_timeout_seconds() -> int:
    """Bound each runner gold query, including result pagination and HTTP waits."""
    setting = "COMMERCE_QUERY_TIMEOUT_SECONDS"
    try:
        timeout = int(os.environ.get(setting, "120"))
    except ValueError as error:
        raise CommercePipelineError(f"{setting} must be an integer") from error
    if not 1 <= timeout <= 86400:
        raise CommercePipelineError(f"{setting} must be between 1 and 86400")
    return timeout


def run_compose_stage(
    service: str, batch: dict[str, str], *, bucket: str,
    journal: CommerceRunJournal | None = None,
) -> None:
    try:
        timeout = int(os.environ.get("COMMERCE_STAGE_TIMEOUT_SECONDS", "1800"))
    except ValueError as error:
        raise CommercePipelineError("COMMERCE_STAGE_TIMEOUT_SECONDS must be an integer") from error
    if not 1 <= timeout <= 86400:
        raise CommercePipelineError("COMMERCE_STAGE_TIMEOUT_SECONDS must be between 1 and 86400")
    container = f"lakeops-commerce-{uuid4().hex}"
    environment = {
        **os.environ, "COMMERCE_BATCH_ID": batch["batch_id"],
        "COMMERCE_MANIFEST_SHA256": batch["manifest_sha256"], "LAKEHOUSE_BUCKET": bucket,
    }
    if service == "bronze-input-sync":
        environment["COMMERCE_SYNC_SCOPE"] = "selected"
    command = ["docker", "compose", "--profile", "catalog", "--profile", "compute",
               "run", "--rm", "--no-deps"]

    def ownership(event: str) -> None:
        if journal:
            journal.record_container(service, container, event, batch=batch,
                                     project_hint=environment.get("COMPOSE_PROJECT_NAME", ""))

    if journal:
        command.extend([
            "--label", f"lakehouse.ops.run_id={journal.run_id}",
            "--label", f"lakehouse.ops.batch_id={batch['batch_id']}",
            "--label", f"lakehouse.ops.manifest_sha256={batch['manifest_sha256']}",
        ])
    command.extend(["--name", container, service])
    ownership("launch_intent")
    try:
        subprocess.run(
            command,
            env=environment, check=True, stdout=sys.stderr, timeout=timeout,
        )
    except (subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        interrupted = isinstance(error, KeyboardInterrupt)
        outcome = "interrupted" if interrupted else "timed out"
        try:
            subprocess.run(
                ["docker", "rm", "--force", container], check=True,
                stdout=sys.stderr, stderr=sys.stderr, timeout=30,
            )
        except (OSError, subprocess.SubprocessError) as cleanup_error:
            ownership("cleanup_unconfirmed")
            raise CommercePipelineError(
                f"commerce stage {outcome}: {service}; container cleanup unconfirmed: {container}"
            ) from cleanup_error
        ownership("interrupted_removed" if interrupted else "timeout_removed")
        if interrupted:
            # Keep Ctrl+C's non-success exit and the run's failed journal event.
            # Removing the Compose client alone does not stop its Docker stage.
            raise
        raise CommercePipelineError(
            f"commerce stage timed out after {timeout}s: {service}; container removed: {container}"
        ) from error
    except (OSError, subprocess.CalledProcessError) as error:
        ownership("failed")
        raise CommercePipelineError(f"commerce stage failed: {service}") from error
    ownership("completed")


def run_commerce_batch(
    planner: CommerceBatchPlanner,
    query: Callable[[str], list[dict[str, Any]]],
    *,
    run_stage: Callable[[str, dict[str, str]], None],
    attempts: int = 1,
    delay_seconds: float = 2,
    clock: Callable[[], float] = time.perf_counter,
    journal: CommerceRunJournal | None = None,
) -> dict[str, Any]:
    validate_commerce_gold_retry(attempts, delay_seconds)
    if journal:
        journal.record("planning", "started")
    try:
        report = _run_commerce_batch(
            planner, query, run_stage=run_stage, attempts=attempts,
            delay_seconds=delay_seconds, clock=clock, journal=journal,
        )
    except BaseException as error:
        if journal:
            journal.record(journal.phase, "failed", error_type=type(error).__name__)
        raise
    if journal:
        journal.record("run", report["status"])
    return report


def _run_commerce_batch(
    planner: CommerceBatchPlanner,
    query: Callable[[str], list[dict[str, Any]]],
    *,
    run_stage: Callable[[str, dict[str, str]], None],
    attempts: int = 1,
    delay_seconds: float = 2,
    clock: Callable[[], float] = time.perf_counter,
    journal: CommerceRunJournal | None = None,
) -> dict[str, Any]:
    started = clock()
    plan = planner.plan(max_batches=1)
    if not plan["batches"]:
        return {"status": "idle", "batch_id": None, "completed_stages": []}
    last = clock()
    planning_seconds = last - started
    batch = plan["batches"][0]
    batch_id = batch["batch_id"]
    if journal:
        journal.record("planning", "selected", batch=batch)
    completed = []
    stage_seconds: dict[str, float] = {}
    for service in COMMERCE_STAGES:
        if journal:
            journal.record(service, "started")
        run_stage(service, batch)
        if journal:
            journal.record(service, "completed")
        now = clock()
        stage_seconds[service] = now - last
        last = now
        completed.append(service)
    try:
        if journal:
            journal.record("verification", "started")
        verification = check_commerce_gold_with_retry(
            query, batch_id, attempts=attempts, delay_seconds=delay_seconds,
        )
    except (CommerceGoldGateError, httpx.HTTPError, TrinoProtocolError, TrinoQueryError) as error:
        raise CommercePipelineError("commerce stage failed: verify-commerce-gold") from error
    now = clock()
    verification_seconds = now - last
    last = now
    if journal:
        journal.record("verification", "completed")
        journal.record("checkpoint", "started")
    checkpoint = planner.commit(batch_id, expected_manifest_sha256=batch["manifest_sha256"])
    if journal:
        journal.record("checkpoint", "completed")
    finished = clock()
    return {
        "status": "ready", "batch_id": batch_id, "completed_stages": completed,
        "verification": verification.as_dict(), "checkpoint": checkpoint,
        "durations_seconds": {
            "planning": planning_seconds, "stages": stage_seconds,
            "verification": verification_seconds, "checkpoint": finished - last,
            "total": finished - started,
        },
    }
