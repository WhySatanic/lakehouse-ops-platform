from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Callable
from typing import Any

import httpx

from lakehouse_ops.commerce_gold_gate import (
    CommerceGoldGateError,
    check_commerce_gold_with_retry,
    validate_commerce_gold_retry,
)
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


def run_compose_stage(service: str, batch: dict[str, str], *, bucket: str) -> None:
    environment = {
        **os.environ, "COMMERCE_BATCH_ID": batch["batch_id"],
        "COMMERCE_MANIFEST_SHA256": batch["manifest_sha256"], "LAKEHOUSE_BUCKET": bucket,
    }
    try:
        subprocess.run(
            ["docker", "compose", "--profile", "catalog", "--profile", "compute",
             "run", "--rm", "--no-deps", service],
            env=environment, check=True, stdout=sys.stderr,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise CommercePipelineError(f"commerce stage failed: {service}") from error


def run_commerce_batch(
    planner: CommerceBatchPlanner,
    query: Callable[[str], list[dict[str, Any]]],
    *,
    run_stage: Callable[[str, dict[str, str]], None],
    attempts: int = 1,
    delay_seconds: float = 2,
    clock: Callable[[], float] = time.perf_counter,
) -> dict[str, Any]:
    validate_commerce_gold_retry(attempts, delay_seconds)
    started = clock()
    plan = planner.plan(max_batches=1)
    if not plan["batches"]:
        return {"status": "idle", "batch_id": None, "completed_stages": []}
    last = clock()
    planning_seconds = last - started
    batch = plan["batches"][0]
    batch_id = batch["batch_id"]
    completed = []
    stage_seconds: dict[str, float] = {}
    for service in COMMERCE_STAGES:
        run_stage(service, batch)
        now = clock()
        stage_seconds[service] = now - last
        last = now
        completed.append(service)
    try:
        verification = check_commerce_gold_with_retry(
            query, batch_id, attempts=attempts, delay_seconds=delay_seconds,
        )
    except (CommerceGoldGateError, httpx.HTTPError, TrinoProtocolError, TrinoQueryError) as error:
        raise CommercePipelineError("commerce stage failed: verify-commerce-gold") from error
    now = clock()
    verification_seconds = now - last
    last = now
    checkpoint = planner.commit(batch_id, expected_manifest_sha256=batch["manifest_sha256"])
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
