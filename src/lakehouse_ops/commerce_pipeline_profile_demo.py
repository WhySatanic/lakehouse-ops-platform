"""Measure two full commerce pipeline volumes on a fresh scoped Docker stack."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lakehouse_ops.commerce_linked_demo import demo


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--orders", type=int, nargs=2, default=[100, 1000])
    args = parser.parse_args()
    try:
        report = demo(Path.cwd(), args.output, scenario="profile", orders=tuple(args.orders))
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())
