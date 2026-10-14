"""Opt-in three-snapshot commerce acceptance through the reference stack."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
from contextlib import redirect_stderr
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import boto3

from lakehouse_ops.commerce_change_scenario import generate_change_scenario
from lakehouse_ops.commerce_pipeline import CURRENT_STAGE, run_commerce_batch, run_compose_stage
from lakehouse_ops.commerce_run_journal import CommerceRunJournal
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock
from lakehouse_ops.ingestion.commerce_batches import CommerceBatchPlanner
from lakehouse_ops.ingestion.commerce_s3_landing import CommerceS3LandingZone
from lakehouse_ops.trino import TrinoClient

CURRENT_TABLE = "lakehouse.gold.commerce_current_daily"


class LinkedStageRunner(Protocol):
    def __call__(
        self, service: str, batch: dict[str, str], *, journal: CommerceRunJournal,
    ) -> None: ...


def utc_timestamp(value: str | None) -> str | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.removesuffix(" UTC"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed.isoformat() + "Z"


def verify_snapshot(
    query: Callable[[str], list[dict[str, Any]]], expected: dict[str, Any], index: int,
    batch_id: str,
) -> dict[str, Any]:
    daily = query(f"SELECT order_day, captured_revenue_cents, order_count, source_batch_id "
                  f"FROM {CURRENT_TABLE} ORDER BY order_day")
    revenue = sum(row["captured_revenue_cents"] for row in daily)
    days = {str(row["order_day"]): row["captured_revenue_cents"] for row in daily}
    wanted_days = (expected["latest_daily_revenue_cents"] if index == 2 else
                   {"2026-01-31": expected["snapshot_revenue_cents"][index]})
    if (days != wanted_days or revenue != expected["snapshot_revenue_cents"][index]
            or sum(row["order_count"] for row in daily) != (2 if index == 2 else 1)
            or any(row["source_batch_id"] != batch_id for row in daily)):
        raise ValueError("current gold differs from literal snapshot oracle")
    history = query(
        "SELECT customer_id, full_name, CAST(valid_from AS varchar) AS valid_from, "
        "CAST(valid_to AS varchar) AS valid_to, is_current "
        "FROM lakehouse.gold.dim_customers_scd2 ORDER BY valid_from"
    )
    actual = []
    for row in history:
        if row["customer_id"] != "C00000001" or row["is_current"] != (row["valid_to"] is None):
            raise ValueError("customer history identity or current flag differs")
        actual.append({"full_name": row["full_name"], **{
            key: utc_timestamp(row[key]) for key in ("valid_from", "valid_to")
        }})
    wanted = expected["customer_history"] if index else [
        {**expected["customer_history"][0], "valid_to": None},
    ]
    if actual != wanted:
        raise ValueError("customer history differs from literal period oracle")
    if index == 2:
        orders = query("SELECT order_id, total_cents FROM lakehouse.silver.commerce_orders "
                       f"WHERE source_batch_id = '{batch_id}' ORDER BY order_id")
        if orders != expected["latest_orders"]:
            raise ValueError("latest orders differ from literal correction oracle")
    return {"batch_id": batch_id, "captured_revenue_cents": revenue,
            "daily_revenue_cents": days, "customer_history": actual}


def exercise_linked_batches(
    planner: CommerceBatchPlanner, query: Callable[[str], list[dict[str, Any]]],
    run_stage: LinkedStageRunner, expected: dict[str, Any],
    output: Path, report: dict[str, Any],
) -> None:
    pending = planner.plan(max_batches=4)["batches"]
    if [batch["batch_id"] for batch in pending] != [
        batch["batch_id"] for batch in expected["batches"]
    ]:
        raise ValueError("requires exactly the three chronological pending scenario batches")
    report["samples"] = []
    for index, batch in enumerate(pending):
        journal = CommerceRunJournal(output / "run-journal.sqlite3")
        sample: dict[str, Any] = {"batch_id": batch["batch_id"], "status": "running"}
        report["samples"].append(sample)

        def stage(
            service: str, selected: dict[str, str], *, wanted_batch=batch,
            run_journal=journal, current_sample=sample, ordinal=index,
        ) -> None:
            if selected["batch_id"] != wanted_batch["batch_id"]:
                raise ValueError("planner selected a different batch")
            run_stage(service, selected, journal=run_journal)
            if service == CURRENT_STAGE:
                current_sample["oracle"] = verify_snapshot(
                    query, expected, ordinal, wanted_batch["batch_id"],
                )

        sample["pipeline"] = run_commerce_batch(
            planner, query, run_stage=stage, attempts=3, journal=journal,
        )
        if sample["pipeline"]["status"] != "ready" or "oracle" not in sample:
            raise ValueError("linked batch did not reach verified checkpoint")
        sample["status"] = "ready"
    snapshots_sql = ('SELECT count(*) AS snapshots FROM lakehouse.gold.'
                     '"commerce_current_daily$snapshots"')
    replay = CommerceRunJournal(output / "run-journal.sqlite3")
    replay.record("replay", "selected", batch=pending[-1])
    try:
        before = query(snapshots_sql)
        replay.record(CURRENT_STAGE, "started")
        run_stage(CURRENT_STAGE, pending[-1], journal=replay)
        replay.record(CURRENT_STAGE, "completed")
        replay.record("verification", "started")
        verify_snapshot(query, expected, 2, pending[-1]["batch_id"])
        if query(snapshots_sql) != before:
            raise ValueError("current mart replay added a snapshot")
        replay.record("verification", "completed")
    except BaseException as error:
        replay.record(replay.phase, "failed", error_type=type(error).__name__)
        raise
    replay.record("run", "ready")

    def no_compute(*_: Any) -> None:
        raise ValueError("idle checkpoint replay started compute")

    report["idle"] = run_commerce_batch(planner, query, run_stage=no_compute)
    if report["idle"]["status"] != "idle":
        raise ValueError("linked scenario did not reach idle")
    report.update(status="ready", current_mart_replay="snapshot_stable",
                  scope="three_full_snapshots_reference_stack")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--s3-endpoint-url", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--late-customer-history", action="store_true",
                        help="After the three batches, run a manual late history correction drill")
    args = parser.parse_args()
    if not os.environ.get("COMPOSE_PROJECT_NAME", "").startswith("lakehouse-ops-linked"):
        parser.error("requires a dedicated lakehouse-ops-linked Compose project")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("requires empty output directory")
    args.output.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"status": "failed", "samples": []}
    try:
        with (
            commerce_runner_lock(),
            TrinoClient(args.server, user="lakehouse-ingestion-ci") as trino,
        ):
            tables = trino.query("SELECT table_name FROM lakehouse.information_schema.tables "
                                 "WHERE table_schema IN ('bronze','silver','gold') AND "
                                 "(table_name LIKE 'commerce_%' "
                                 "OR table_name='dim_customers_scd2')")
            if tables:
                raise ValueError("requires fresh commerce catalog")
            generate_change_scenario(args.output / "scenario")
            expected = json.loads((args.output / "scenario" / "expected.json").read_text())
            s3 = boto3.client("s3", endpoint_url=args.s3_endpoint_url, region_name="us-east-1")
            landing = CommerceS3LandingZone(s3, bucket="lakehouse")
            report["landing"] = []
            for batch in expected["batches"]:
                root = args.output / "scenario" / f"batch_id={batch['batch_id']}"
                first = landing.write(root)
                if landing.write(root).created != 0:
                    raise ValueError("conditional source replay created new objects")
                report["landing"].append(first.as_dict())
            planner = CommerceBatchPlanner(s3, bucket="lakehouse",
                                           state_path=args.output / "checkpoint.json")
            with (
                (args.output / "stages.log").open("w", encoding="utf-8") as log,
                redirect_stderr(log),
            ):
                exercise_linked_batches(
                    planner, trino.query,
                    lambda service, batch, *, journal: run_compose_stage(
                        service, batch, bucket="lakehouse", journal=journal,
                    ),
                    expected, args.output, report,
                )
                if args.late_customer_history:
                    from lakehouse_ops.commerce_history_correction import (
                        exercise_following_batch,
                        exercise_history_correction,
                        exercise_late_batch_completion,
                        generate_following_observation,
                        generate_late_observation,
                    )

                    batch = generate_late_observation(
                        args.output / "scenario" / f"batch_id={expected['batches'][0]['batch_id']}",
                        args.output / "late-observation",
                    )
                    report["late_customer_landing"] = landing.write(
                        args.output / "late-observation",
                    ).as_dict()
                    if landing.write(args.output / "late-observation").created:
                        raise ValueError("late customer source replay created new objects")

                    def history_stage(service, selected, *, journal, history_mode):
                        previous = os.environ.get("COMMERCE_CUSTOMER_HISTORY_MODE")
                        os.environ["COMMERCE_CUSTOMER_HISTORY_MODE"] = history_mode
                        try:
                            run_compose_stage(
                                service, selected, bucket="lakehouse", journal=journal,
                            )
                        finally:
                            if previous is None:
                                os.environ.pop("COMMERCE_CUSTOMER_HISTORY_MODE", None)
                            else:
                                os.environ["COMMERCE_CUSTOMER_HISTORY_MODE"] = previous

                    exercise_history_correction(
                        trino.query, history_stage, batch, expected, args.output, report,
                    )
                    exercise_late_batch_completion(
                        planner, trino.query,
                        lambda service, selected, *, journal: run_compose_stage(
                            service, selected, bucket="lakehouse", journal=journal,
                        ), batch, expected, args.output, report,
                    )
                    report["idle_after_correction"] = run_commerce_batch(
                        planner, trino.query, run_stage=lambda *_: None,
                    )
                    if report["idle_after_correction"]["status"] != "idle":
                        raise ValueError("late correction left a pending batch")
                    latest_id = expected["batches"][-1]["batch_id"]
                    following = generate_following_observation(
                        args.output / "scenario" / f"batch_id={latest_id}",
                        args.output / "following-observation",
                    )
                    report["following_landing"] = landing.write(
                        args.output / "following-observation",
                    ).as_dict()
                    if landing.write(args.output / "following-observation").created:
                        raise ValueError("following source replay created new objects")
                    exercise_following_batch(
                        planner, trino.query,
                        lambda service, selected, *, journal: run_compose_stage(
                            service, selected, bucket="lakehouse", journal=journal,
                        ), following, expected, args.output, report,
                    )
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__)
        print(f"linked commerce acceptance failed: {error}", file=sys.stderr)
        return 2
    finally:
        (args.output / "linked.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
