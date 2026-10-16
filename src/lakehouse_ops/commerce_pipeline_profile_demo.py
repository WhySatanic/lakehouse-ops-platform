"""Measure commerce volumes and snapshot history on a fresh scoped Docker stack."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lakehouse_ops.commerce_linked_demo import demo


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--orders", type=int, nargs=2, default=[100, 1000])
    parser.add_argument("--customers", type=int, help="customers per snapshot (2..100000)")
    parser.add_argument("--batches", type=int, help="chronological full snapshots (2..100)")
    parser.add_argument("--customer-revisions", type=int, nargs="+",
                        help="one attribute revision 0..10000 per snapshot; verifies SCD2")
    args = parser.parse_args()
    workload = {name: getattr(args, name) for name in (
        "customers", "batches", "customer_revisions",
    ) if getattr(args, name) is not None}
    try:
        report = demo(Path.cwd(), args.output, scenario="profile", orders=tuple(args.orders),
                      **workload)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())
