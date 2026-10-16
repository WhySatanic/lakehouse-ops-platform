"""Opt-in three-snapshot commerce acceptance through the reference stack."""

from __future__ import annotations

import argparse
import hashlib
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
from lakehouse_ops.commerce_gold_gate import check_commerce_gold
from lakehouse_ops.commerce_pipeline import (
    COMMERCE_STAGES,
    CURRENT_STAGE,
    run_commerce_batch,
    run_compose_stage,
)
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


def verify_customer_history(query, wanted):
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
    if actual != wanted:
        raise ValueError("customer history differs from literal period oracle")
    return actual


def verify_snapshot(
    query: Callable[[str], list[dict[str, Any]]], expected: dict[str, Any], index: int,
    batch_id: str,
) -> dict[str, Any]:
    daily = query(f"SELECT order_day, captured_revenue_cents, order_count, source_batch_id "
                  f"FROM {CURRENT_TABLE} ORDER BY order_day")
    revenue = sum(row["captured_revenue_cents"] for row in daily)
    days = {str(row["order_day"]): row["captured_revenue_cents"] for row in daily}
    refund = expected.get("scenario") == "refund"
    wanted_days = (expected["snapshot_daily_revenue_cents"][index] if refund else
                   expected["latest_daily_revenue_cents"] if index == 2 else
                   {"2026-01-31": expected["snapshot_revenue_cents"][index]})
    wanted_count = expected["snapshot_order_counts"][index] if refund else (2 if index == 2 else 1)
    if (days != wanted_days or revenue != expected["snapshot_revenue_cents"][index]
            or sum(row["order_count"] for row in daily) != wanted_count
            or any(row["source_batch_id"] != batch_id for row in daily)):
        raise ValueError("current gold differs from literal snapshot oracle")
    wanted = expected["customer_history"] if index else [
        {**expected["customer_history"][0], "valid_to": None},
    ]
    actual = verify_customer_history(query, wanted)
    if index == len(expected["batches"]) - 1:
        orders = query("SELECT order_id, total_cents FROM lakehouse.silver.commerce_orders "
                       f"WHERE source_batch_id = '{batch_id}' ORDER BY order_id")
        if orders != expected["latest_orders"]:
            raise ValueError("latest orders differ from literal correction oracle")
    result = {"batch_id": batch_id, "captured_revenue_cents": revenue,
              "daily_revenue_cents": days, "customer_history": actual}
    if refund:
        historical = query(
            "SELECT source_batch_id, CAST(order_day AS varchar) AS order_day, "
            "captured_revenue_cents, order_count FROM lakehouse.gold.commerce_daily "
            "ORDER BY source_batch_id, order_day"
        )
        wanted_history = sorted([
            {"source_batch_id": batch["batch_id"], "order_day": day,
             "captured_revenue_cents": value, "order_count": 1}
            for ordinal, batch in enumerate(expected["batches"][:index + 1])
            for day, value in expected["snapshot_daily_revenue_cents"][ordinal].items()
        ], key=lambda row: (row["source_batch_id"], row["order_day"]))
        if historical != wanted_history:
            raise ValueError("historical gold differs from literal refund oracle")
        payments = query(
            "SELECT source_batch_id, payment_id, status, amount_cents "
            "FROM lakehouse.silver.commerce_payments ORDER BY source_batch_id, payment_id"
        )
        wanted_payments = sorted([
            {"source_batch_id": batch["batch_id"], "payment_id": payment_id,
             "status": status, "amount_cents": amount}
            for ordinal, batch in enumerate(expected["batches"][:index + 1])
            for payment_id, status, amount in expected["snapshot_payment_states"][ordinal]
        ], key=lambda row: (row["source_batch_id"], row["payment_id"]))
        if payments != wanted_payments:
            raise ValueError("payment history differs from literal refund oracle")
        result.update(historical_daily=historical, payment_history=payments)
    return result


def replay_current_snapshot(query, run_stage, expected, index, batch, output):
    snapshots_sql = ('SELECT count(*) AS snapshots FROM lakehouse.gold.'
                     '"commerce_current_daily$snapshots"')
    replay = CommerceRunJournal(output / "run-journal.sqlite3")
    replay.record("replay", "selected", batch=batch)
    try:
        before = query(snapshots_sql)
        replay.record(CURRENT_STAGE, "started")
        run_stage(CURRENT_STAGE, batch, journal=replay)
        replay.record(CURRENT_STAGE, "completed")
        replay.record("verification", "started")
        oracle = verify_snapshot(query, expected, index, batch["batch_id"])
        if query(snapshots_sql) != before:
            raise ValueError("current mart replay added a snapshot")
        replay.record("verification", "completed")
    except BaseException as error:
        replay.record(replay.phase, "failed", error_type=type(error).__name__)
        raise
    replay.record("run", "ready")
    return {"status": "ready", "snapshots": before, "oracle": oracle}


def exercise_linked_batches(
    planner: CommerceBatchPlanner, query: Callable[[str], list[dict[str, Any]]],
    run_stage: LinkedStageRunner, expected: dict[str, Any],
    output: Path, report: dict[str, Any],
) -> None:
    pending = planner.plan(max_batches=len(expected["batches"]) + 1)["batches"]
    if [batch["batch_id"] for batch in pending] != [
        batch["batch_id"] for batch in expected["batches"]
    ]:
        raise ValueError("requires exactly the three chronological pending scenario batches"
                         if expected.get("scenario") != "refund" else
                         "requires exactly five chronological pending refund batches")
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
        if expected.get("scenario") == "refund" and index == 2:
            report["refund_replay"] = replay_current_snapshot(
                query, run_stage, expected, index, batch, output,
            )
    replay_current_snapshot(query, run_stage, expected, len(pending) - 1, pending[-1], output)

    def no_compute(*_: Any) -> None:
        raise ValueError("idle checkpoint replay started compute")

    report["idle"] = run_commerce_batch(planner, query, run_stage=no_compute)
    if report["idle"]["status"] != "idle":
        raise ValueError("linked scenario did not reach idle")
    report.update(status="ready", current_mart_replay="snapshot_stable",
                  scope=("five_full_snapshots_refund_reference_stack"
                         if expected.get("scenario") == "refund" else
                         "three_full_snapshots_reference_stack"))


def exercise_inflight_arrival(
    new_planner: Callable[[], CommerceBatchPlanner],
    query: Callable[[str], list[dict[str, Any]]], run_stage: LinkedStageRunner,
    expected: dict[str, Any], output: Path, report: dict[str, Any],
    land_late: Callable[[], None], land_following: Callable[[], dict[str, str]],
) -> None:
    """Recover C's published history before processing B that arrived during C."""
    class InflightDrillFailure(RuntimeError):
        pass

    first, late, newer = expected["batches"]
    checkpoint = output / "checkpoint.json"
    history_sql = ('SELECT count(*) AS snapshots FROM lakehouse.gold.'
                   '"dim_customers_scd2$snapshots"')
    interim = {**expected, "customer_history": [
        {**expected["customer_history"][0], "valid_to": newer["batch_at"]},
        {**expected["customer_history"][1], "valid_from": newer["batch_at"]},
    ]}
    sample: dict[str, Any] = {"status": "running", "attempts": [],
                              "failure_boundary": "after_C_history_before_gold_checkpoint"}
    report["inflight_recovery"] = sample
    following = None
    history_before_following = None
    planner = new_planner()
    if [item["batch_id"] for item in planner.plan(max_batches=3)["batches"]] != [
        first["batch_id"], newer["batch_id"],
    ]:
        raise ValueError("in-flight drill requires only A and C landed initially")

    def attempt(batch, mode, oracle_expected, ordinal, current_id, *, inject=False):
        journal = CommerceRunJournal(output / "run-journal.sqlite3")
        item = {"batch_id": batch["batch_id"], "run_id": journal.run_id, "status": "running"}
        sample["attempts"].append(item)

        def stage(service, selected):
            if (selected["batch_id"] != batch["batch_id"]
                    or selected.get("customer_history_mode") != mode):
                raise ValueError("in-flight queue selected a wrong batch or history mode")
            run_stage(service, selected, journal=journal)
            if inject and service == "spark-commerce-customer-scd2":
                sample["published_C_history"] = verify_customer_history(
                    query, interim["customer_history"],
                )
                sample["history_after_publication"] = query(history_sql)
                if sample["history_after_publication"][0]["snapshots"] != (
                    sample["history_after_A"][0]["snapshots"] + 1
                ):
                    raise ValueError("C did not publish exactly one atomic history commit")
                land_late()
                raise InflightDrillFailure("B landed after C history publication")
            if service == CURRENT_STAGE:
                item["oracle"] = verify_snapshot(
                    query, oracle_expected, ordinal, current_id,
                )
                item["batch_gold_oracle"] = check_commerce_gold(query, batch["batch_id"]).as_dict()
                wanted_revenue = expected["snapshot_revenue_cents"][
                    0 if batch["batch_id"] == first["batch_id"] else
                    1 if batch["batch_id"] == late["batch_id"] else 2
                ]
                if item["batch_gold_oracle"]["captured_revenue_cents"] != wanted_revenue:
                    raise ValueError("batch gold differs from literal in-flight revenue oracle")
                if (batch["batch_id"] == newer["batch_id"] and not inject
                        and query(history_sql) != sample["history_after_publication"]):
                    raise ValueError("C history replay added a snapshot")
                if (batch["batch_id"] == late["batch_id"]
                        and query(history_sql)[0]["snapshots"] != (
                        sample["history_after_publication"][0]["snapshots"] + 1
                        )):
                    raise ValueError("late rebuild did not publish exactly one history commit")
                if (following is not None and batch["batch_id"] == following["batch_id"]
                        and query(history_sql) != history_before_following):
                    raise ValueError("unchanged following customer added a history snapshot")
        try:
            item["pipeline"] = run_commerce_batch(
                planner, query, run_stage=stage, attempts=3, journal=journal,
            )
        except InflightDrillFailure:
            item["status"] = "injected_failure"
            raise
        if (item["pipeline"]["status"] != "ready" or "oracle" not in item
                or item["pipeline"]["completed_stages"] != [*COMMERCE_STAGES, CURRENT_STAGE]):
            raise ValueError("in-flight batch did not finish all stages and SQL before checkpoint")
        item["status"] = "ready"

    attempt(first, "forward", expected, 0, first["batch_id"])
    sample["history_after_A"] = query(history_sql)
    before = checkpoint.read_bytes()
    sample["checkpoint_before_failure_sha256"] = hashlib.sha256(before).hexdigest()
    try:
        attempt(newer, "forward", interim, 2, newer["batch_id"], inject=True)
    except InflightDrillFailure:
        pass
    else:
        raise ValueError("in-flight failure boundary was not reached")
    after = checkpoint.read_bytes()
    sample["checkpoint_after_failure_sha256"] = hashlib.sha256(after).hexdigest()
    if after != before:
        raise ValueError("partially published C advanced checkpoint")
    planner = new_planner()  # Recover from persisted intent, not the failed instance's memory.
    attempt(newer, "forward", interim, 2, newer["batch_id"])
    attempt(late, "rebuild", expected, 2, newer["batch_id"])
    history_before_following = query(history_sql)
    following = land_following()
    attempt(following, "forward", expected, 2, following["batch_id"])

    def no_compute(*_):
        raise ValueError("in-flight idle started compute")

    sample["idle"] = run_commerce_batch(planner, query, run_stage=no_compute)
    if sample["idle"]["status"] != "idle":
        raise ValueError("in-flight queue did not reach idle")
    processed = json.loads(checkpoint.read_text())["processed_batches"]
    if set(processed) != {batch["batch_id"] for batch in (first, newer, late, following)}:
        raise ValueError("in-flight checkpoint does not contain exactly the four complete batches")
    sample.update(status="ready", completed_batches=list(processed),
                  checkpoint_sha256=hashlib.sha256(checkpoint.read_bytes()).hexdigest())
    report.update(status="ready", scope="inflight_late_arrival_reference_stack")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--s3-endpoint-url", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--refund", action="store_true",
                        help="Verify five complete payment correction/refund snapshots")
    parser.add_argument("--late-customer-history", action="store_true",
                        help="After the three batches, run a manual late history correction drill")
    parser.add_argument("--inflight-late-arrival", action="store_true",
                        help="Land B during C publication, fail C, then verify retry/B/next/idle")
    args = parser.parse_args()
    if args.inflight_late_arrival and args.late_customer_history:
        parser.error("choose either in-flight arrival or the manual history correction drill")
    if args.refund and (args.inflight_late_arrival or args.late_customer_history):
        parser.error("refund acceptance cannot be combined with a recovery drill")
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
            generate_change_scenario(args.output / "scenario",
                                     scenario="refund" if args.refund else "correction")
            expected = json.loads((args.output / "scenario" / "expected.json").read_text())
            s3 = boto3.client("s3", endpoint_url=args.s3_endpoint_url, region_name="us-east-1")
            landing = CommerceS3LandingZone(s3, bucket="lakehouse")
            report["landing"] = []
            for batch in expected["batches"]:
                if args.inflight_late_arrival and batch == expected["batches"][1]:
                    continue
                root = args.output / "scenario" / f"batch_id={batch['batch_id']}"
                first = landing.write(root)
                if landing.write(root).created != 0:
                    raise ValueError("conditional source replay created new objects")
                report["landing"].append(first.as_dict())
            planner = CommerceBatchPlanner(s3, bucket="lakehouse",
                                           state_path=args.output / "checkpoint.json",
                                           manifest_read_workers=8)
            with (
                (args.output / "stages.log").open("w", encoding="utf-8") as log,
                redirect_stderr(log),
            ):
                if args.inflight_late_arrival:
                    from lakehouse_ops.commerce_history_correction import (
                        generate_following_observation,
                    )

                    def land_late():
                        late_id = expected["batches"][1]["batch_id"]
                        root = args.output / "scenario" / f"batch_id={late_id}"
                        report["inflight_late_landing"] = landing.write(root).as_dict()
                        if landing.write(root).created:
                            raise ValueError("in-flight late source replay created new objects")

                    def land_following():
                        latest = expected["batches"][-1]["batch_id"]
                        following = generate_following_observation(
                            args.output / "scenario" / f"batch_id={latest}",
                            args.output / "following-observation",
                        )
                        report["following_landing"] = landing.write(
                            args.output / "following-observation",
                        ).as_dict()
                        return following

                    exercise_inflight_arrival(
                        lambda: CommerceBatchPlanner(s3, bucket="lakehouse",
                                                     state_path=args.output / "checkpoint.json",
                                                     manifest_read_workers=8),
                        trino.query,
                        lambda service, batch, *, journal: run_compose_stage(
                            service, batch, bucket="lakehouse", journal=journal,
                        ), expected, args.output, report, land_late, land_following,
                    )
                else:
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
