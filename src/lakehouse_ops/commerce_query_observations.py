"""Read-only repeated detail/mart query observations on a retained commerce catalog."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from statistics import median

from lakehouse_ops.commerce_pipeline_profile import query_costs, snapshot_inventory
from lakehouse_ops.trino import TrinoClient

METRICS = (
    "elapsed_time_ms", "wall_time_ms", "cpu_time_ms", "processed_rows", "processed_bytes",
    "physical_input_bytes", "peak_memory_bytes", "spilled_bytes",
)


def observe(client, output: Path, state: Path, *, batch_id: str, orders: int, repetitions=4):
    if re.fullmatch(r"[0-9a-f]{16}", batch_id) is None:
        raise ValueError("requires a fixture batch ID")
    if isinstance(orders, bool) or not 100 <= orders <= 10_000:
        raise ValueError("requires a fixture volume of 100..10000 orders")
    if isinstance(repetitions, bool) or repetitions not in (2, 4, 6, 8, 10):
        raise ValueError("requires 2..10 even repetitions for balanced query order")
    checkpoint = state.read_bytes()
    if output.exists() and any(output.iterdir()):
        raise ValueError("requires an empty evidence directory")
    output.mkdir(parents=True, exist_ok=True)
    report = {"status": "running", "scope": "repeated_warm_queries_not_pipeline_capacity",
              "batch_id": batch_id, "orders": orders, "repetitions": repetitions,
              "samples": [], "limitations": ["no cold-cache guarantee",
              "deterministic alternating order, not randomized",
              "shared host and sequential queries, not concurrent throughput"]}
    try:
        report["snapshots_before"] = snapshot_inventory(client)
        report["warmup"] = query_costs(client, batch_id, orders)
        expected = report["warmup"]["detail"]["rows"]
        for index in range(repetitions):
            costs = query_costs(client, batch_id, orders, daily_first=bool(index % 2))
            report["samples"].append({"ordinal": index + 1,
                                      "order": ["daily", "detail"] if index % 2
                                      else ["detail", "daily"], **costs})
            if costs["detail"]["rows"] != expected:
                raise ValueError("query results changed between repetitions")
            (output / "queries.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        report["snapshots_after"] = snapshot_inventory(client)
        if report["snapshots_before"] != report["snapshots_after"]:
            raise ValueError("commerce snapshots changed during observation")
        if state.read_bytes() != checkpoint:
            raise ValueError("processing checkpoint changed during observation")
        report["metrics"] = {}
        for name in ("detail", "daily"):
            report["metrics"][name] = {}
            for metric in METRICS:
                values = [sample[name]["stats"][metric] for sample in report["samples"]]
                report["metrics"][name][metric] = {
                    "median": median(values), "min": min(values), "max": max(values),
                }
        report.update(status="ready", checkpoint="unchanged", snapshots="unchanged",
                      measured_queries=2 * repetitions, warmup_queries=2)
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__)
        raise
    finally:
        (output / "queries.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--server", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--orders", required=True, type=int)
    parser.add_argument("--repetitions", type=int, default=4)
    args = parser.parse_args()
    try:
        with TrinoClient(args.server, user="lakehouse-performance", query_timeout=60) as client:
            report = observe(client, args.output, args.state, batch_id=args.batch_id,
                             orders=args.orders, repetitions=args.repetitions)
    except Exception as error:
        print(f"commerce query observations failed: {type(error).__name__}", file=sys.stderr)
        return 2
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
