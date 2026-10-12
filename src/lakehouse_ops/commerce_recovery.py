"""Opt-in recovery acceptance after gold publication and before checkpoint."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Callable
from contextlib import redirect_stderr
from pathlib import Path
from typing import Any

import boto3

from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES, run_commerce_batch, run_compose_stage
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock
from lakehouse_ops.ingestion.commerce_batches import CommerceBatchPlanner
from lakehouse_ops.ingestion.commerce_fixture import (
    CommerceFixtureConfig,
    generate_commerce_fixture,
)
from lakehouse_ops.ingestion.commerce_s3_landing import CommerceS3LandingZone
from lakehouse_ops.trino import TrinoClient


class InjectedCommerceInterruption(RuntimeError):
    pass


def audit_tables(query: Callable[[str], list[dict[str, Any]]]) -> list[dict[str, Any]]:
    tables = query(
        "SELECT table_schema, table_name FROM lakehouse.information_schema.tables "
        "WHERE table_schema IN ('bronze','silver','gold') "
        "AND (table_name LIKE 'commerce_%' OR table_name = 'dim_customers_scd2') "
        "ORDER BY table_schema, table_name"
    )
    audit = []
    for table in tables:
        schema = table["table_schema"].replace('"', '""')
        name = table["table_name"].replace('"', '""')
        rows = query(f'SELECT * FROM lakehouse."{schema}"."{name}"')
        canonical = sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)
        digest = hashlib.sha256("\n".join(canonical).encode()).hexdigest()
        snapshots = query(
            f'SELECT count(*) AS snapshots FROM lakehouse."{schema}"."{name}$snapshots"'
        )[0]["snapshots"]
        files = query(
            f'SELECT content, file_path, record_count, file_size_in_bytes '
            f'FROM lakehouse."{schema}"."{name}$files"'
        )
        files_digest = hashlib.sha256("\n".join(sorted(
            json.dumps(row, sort_keys=True, default=str) for row in files
        )).encode()).hexdigest()
        audit.append({"table": f"{schema}.{name}", "rows": len(rows),
                      "rows_sha256": digest, "files_sha256": files_digest,
                      "files": len(files), "snapshots": snapshots})
    return audit


def exercise_recovery(
    planner: CommerceBatchPlanner,
    query: Callable[[str], list[dict[str, Any]]],
    run_stage: Callable[[str, dict[str, str]], None],
    audit: Callable[[], list[dict[str, Any]]],
    state: Path,
    *,
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence = {} if evidence is None else evidence
    selected = planner.plan(max_batches=2)["batches"]
    if len(selected) != 1 or state.exists():
        raise ValueError("requires one pending batch and a fresh checkpoint")
    first_stages = []

    def interrupt(service: str, batch: dict[str, str]) -> None:
        run_stage(service, batch)
        first_stages.append(service)
        if service == COMMERCE_STAGES[-1]:
            raise InjectedCommerceInterruption("after gold publication, before verification")

    try:
        run_commerce_batch(planner, query, run_stage=interrupt, attempts=3)
    except InjectedCommerceInterruption:
        pass
    else:
        raise ValueError("interruption was not reached")
    if first_stages != list(COMMERCE_STAGES) or state.exists():
        raise ValueError("premature checkpoint or incomplete injection boundary")
    if planner.plan(max_batches=1)["batches"] != selected:
        raise ValueError("interrupted batch is no longer pending")
    before = audit()
    evidence.update(checkpoint_before_retry="absent", published_before_retry=before)
    if not before:
        raise ValueError("gold publication produced no audit evidence")
    retry = run_commerce_batch(planner, query, run_stage=run_stage, attempts=3)
    if retry["status"] != "ready" or retry["batch_id"] != selected[0]["batch_id"]:
        raise ValueError("retry did not complete the interrupted batch")
    after = audit()
    evidence.update(published_after_retry=after, retry=retry)
    # Iceberg may commit an empty MERGE snapshot. Data and physical files must
    # remain identical, but snapshot count is observational, not an invariant.
    def published(audits: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [{key: value for key, value in item.items() if key != "snapshots"}
                for item in audits]

    if published(before) != published(after):
        raise ValueError("retry changed published rows or Iceberg files")
    if any(new["snapshots"] < old["snapshots"] for old, new in zip(before, after, strict=True)):
        raise ValueError("retry removed Iceberg snapshot history")
    def idle_stage(*_: Any) -> None:
        raise ValueError("idle replay started compute")

    idle = run_commerce_batch(planner, query, run_stage=idle_stage)
    if idle["status"] != "idle":
        raise ValueError("checkpoint replay was not idle")
    return {
        "status": "ready", "scope": "caught_exception_after_gold_before_verification",
        "batch_id": selected[0]["batch_id"], "interrupted_stages": first_stages,
        "checkpoint_before_retry": "absent", "published_before_retry": before,
        "published_after_retry": after, "retry": retry, "idle": idle,
        "limitations": ["not SIGKILL/OOM", "not failure inside an Iceberg commit",
                        "not distributed exactly-once recovery",
                        "empty MERGE snapshots may be added on retry"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--s3-endpoint-url", required=True)
    parser.add_argument("--server", required=True)
    args = parser.parse_args()
    project = os.environ.get("COMPOSE_PROJECT_NAME", "")
    if not project.startswith("lakehouse-ops-recovery"):
        parser.error("requires a dedicated lakehouse-ops-recovery Compose project")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("requires empty output directory")
    args.output.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"status": "failed"}
    try:
        with (
            commerce_runner_lock(),
            TrinoClient(args.server, user="lakehouse-recovery-drill") as trino,
        ):
            if audit_tables(trino.query):
                raise ValueError("requires fresh commerce catalog")
            s3 = boto3.client("s3", endpoint_url=args.s3_endpoint_url, region_name="us-east-1")
            fixture = generate_commerce_fixture(args.output / "fixture", CommerceFixtureConfig(
                customers=10, products=5, orders=100, null_customer_emails=1,
                duplicate_orders=1, late_orders=1, invalid_payments=1,
                batch_at="2026-02-03T00:00:00+00:00",
            ))
            CommerceS3LandingZone(s3, bucket="lakehouse").write(fixture.path)
            state = args.output / "checkpoint.json"
            planner = CommerceBatchPlanner(s3, bucket="lakehouse", state_path=state)
            with (
                (args.output / "stages.log").open("w", encoding="utf-8") as log,
                redirect_stderr(log),
            ):
                report = exercise_recovery(
                    planner, trino.query,
                    lambda service, batch: run_compose_stage(service, batch, bucket="lakehouse"),
                    lambda: audit_tables(trino.query), state, evidence=report,
                )
    except Exception as error:
        report["error_type"] = type(error).__name__
        print(f"commerce recovery acceptance failed: {error}", file=sys.stderr)
        return 2
    finally:
        (args.output / "recovery.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
