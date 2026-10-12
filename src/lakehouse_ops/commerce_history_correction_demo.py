"""Demonstrate three loads and late customer history correction on a fresh local stack."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lakehouse_ops.commerce_linked_demo import demo


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        report = demo(Path.cwd(), args.output, scenario="linked-history")
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())
