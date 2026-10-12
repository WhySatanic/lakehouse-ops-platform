"""Opt-in forced runner-owner termination at a completed gold boundary."""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import sqlite3
import time
from contextlib import redirect_stderr
from pathlib import Path
from typing import Any

import boto3

from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES, run_commerce_batch, run_compose_stage
from lakehouse_ops.commerce_recovery import audit_tables
from lakehouse_ops.commerce_run_journal import CommerceRunJournal
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock
from lakehouse_ops.ingestion.commerce_batches import CommerceBatchPlanner
from lakehouse_ops.ingestion.commerce_fixture import (
    CommerceFixtureConfig,
    generate_commerce_fixture,
)
from lakehouse_ops.ingestion.commerce_s3_landing import CommerceS3LandingZone
from lakehouse_ops.trino import TrinoClient


def force_owner_exit(target: Any, args: tuple, *, timeout: float = 1800) -> int:
    """Terminate only the spawned owner, and only accept its completed-stage marker."""
    context = mp.get_context("spawn")
    reader, writer = context.Pipe(duplex=False)
    process = context.Process(target=target, args=(writer, *args))
    process.start()
    writer.close()
    try:
        if not reader.poll(timeout):
            raise TimeoutError("owner did not reach completed gold boundary; no retry allowed")
        try:
            marker = reader.recv()
        except EOFError as error:
            raise RuntimeError("owner exited before completed gold boundary") from error
        if marker != {"boundary": "gold_published", "pid": process.pid}:
            raise ValueError("unexpected owner boundary marker")
        process.kill()
        process.join(15)
        if process.is_alive() or process.exitcode == 0:
            raise RuntimeError("forced owner termination not confirmed")
        return process.exitcode
    finally:
        if process.is_alive():
            process.kill()
            process.join(15)
        reader.close()
        if not process.is_alive():
            process.close()


def owner(connection: Any, output: Path, endpoint: str, server: str) -> None:
    with (
        commerce_runner_lock(),
        TrinoClient(server, user="lakehouse-recovery-drill") as trino,
        (output / "owner.log").open("w", encoding="utf-8") as log,
        redirect_stderr(log),
    ):
        s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")
        planner = CommerceBatchPlanner(
            s3, bucket="lakehouse", state_path=output / "checkpoint.json",
        )
        journal = CommerceRunJournal(output / "journal.sqlite3")

        def stage(service: str, batch: dict[str, str]) -> None:
            run_compose_stage(service, batch, bucket="lakehouse", journal=journal)
            if service == COMMERCE_STAGES[-1]:
                connection.send({"boundary": "gold_published", "pid": os.getpid()})
                # The parent kills this owner. No finally/exception path advances state.
                time.sleep(3600)
                raise RuntimeError("parent did not terminate paused owner")

        run_commerce_batch(planner, trino.query, run_stage=stage,
                           journal=journal)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--s3-endpoint-url", required=True)
    parser.add_argument("--server", required=True)
    args = parser.parse_args()
    if not os.environ.get("COMPOSE_PROJECT_NAME", "").startswith("lakehouse-ops-owner-exit"):
        parser.error("requires dedicated lakehouse-ops-owner-exit project")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("requires empty output")
    args.output.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"status": "failed", "scope": "owner_kill_after_completed_gold"}
    state = args.output / "checkpoint.json"
    try:
        with commerce_runner_lock(), TrinoClient(
            args.server, user="lakehouse-recovery-drill",
        ) as trino:
            if audit_tables(trino.query):
                raise ValueError("requires fresh commerce catalog")
            s3 = boto3.client("s3", endpoint_url=args.s3_endpoint_url, region_name="us-east-1")
            fixture = generate_commerce_fixture(args.output / "fixture", CommerceFixtureConfig(
                customers=10, products=5, orders=100, null_customer_emails=1,
                duplicate_orders=1, late_orders=1, invalid_payments=1,
                batch_at="2026-02-03T00:00:00+00:00",
            ))
            CommerceS3LandingZone(s3, bucket="lakehouse").write(fixture.path)
        report["owner_exit_code"] = force_owner_exit(
            owner, (args.output, args.s3_endpoint_url, args.server),
        )
        # Reacquisition proves OS-lock release without deleting a possibly live lock file.
        with commerce_runner_lock(), TrinoClient(
            args.server, user="lakehouse-recovery-drill",
        ) as trino:
            if state.exists():
                raise ValueError("terminated owner advanced checkpoint")
            with sqlite3.connect(args.output / "journal.sqlite3") as db:
                first = db.execute(
                    "SELECT run_id,phase,event FROM commerce_run_events ORDER BY sequence"
                ).fetchall()
            if not first or first[-1][1:] != (COMMERCE_STAGES[-1], "started"):
                raise ValueError("incomplete owner journal boundary missing")
            report.update(checkpoint_before_retry="absent", incomplete_owner_events=first)
            before = audit_tables(trino.query)
            report["published_before_retry"] = before
            if not before:
                raise ValueError("completed publication has no tables")
            planner = CommerceBatchPlanner(s3, bucket="lakehouse", state_path=state)
            journal = CommerceRunJournal(args.output / "journal.sqlite3")
            with (
                (args.output / "retry.log").open("w", encoding="utf-8") as log,
                redirect_stderr(log),
            ):
                retry = run_commerce_batch(
                    planner, trino.query, attempts=3, journal=journal,
                    run_stage=lambda service, batch: run_compose_stage(
                        service, batch, bucket="lakehouse", journal=journal,
                    ),
                )
            after = audit_tables(trino.query)
            report.update(retry=retry, published_after_retry=after)
            def data(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
                return [{k: v for k, v in row.items() if k != "snapshots"} for row in rows]

            if (retry["status"] != "ready" or retry["batch_id"] != fixture.batch_id
                    or data(before) != data(after)):
                raise ValueError("retry changed published rows/files or selected batch")
            if journal.run_id == first[0][0]:
                raise ValueError("retry reused terminated owner run ID")
            def no_compute(*_: Any) -> None:
                raise ValueError("idle started compute")

            idle = run_commerce_batch(planner, trino.query, run_stage=no_compute)
            if idle["status"] != "idle":
                raise ValueError("retry did not commit checkpoint")
            report.update(status="ready", idle=idle)
    except Exception as error:
        report.update(error_type=type(error).__name__, error=str(error))
        return 2
    finally:
        (args.output / "owner-exit.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
