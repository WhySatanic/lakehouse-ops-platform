"""Measure deterministic commerce fixture generation at two source volumes."""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
import tracemalloc
from dataclasses import asdict
from pathlib import Path
from typing import Any

from lakehouse_ops.ingestion.commerce_fixture import (
    CommerceFixtureConfig,
    generate_commerce_fixture,
)


def _config(orders: int) -> CommerceFixtureConfig:
    customers = max(10, orders // 10)
    return CommerceFixtureConfig(
        customers=customers,
        products=max(5, orders // 100),
        orders=orders,
        null_customer_emails=max(1, customers // 10),
        duplicate_orders=max(1, orders // 100),
        late_orders=max(1, orders // 100),
        invalid_payments=max(1, orders // 100),
    )


def _profile(root: Path, label: str, orders: int) -> dict[str, Any]:
    config = _config(orders)
    tracemalloc.start()
    try:
        started = time.perf_counter()
        result = generate_commerce_fixture(root / label, config)
        elapsed = time.perf_counter() - started
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    if not result.created:
        raise ValueError("scale profile requires fresh fixture generation")
    replay = generate_commerce_fixture(root / label, config)
    if replay.created or replay.batch_id != result.batch_id or replay.tables != result.tables:
        raise ValueError("fixture replay did not verify the same content")
    names = ("customers", "products", "orders", "payments")
    jsonl_bytes = sum((result.path / f"{name}.jsonl").stat().st_size for name in names)
    return {
        "label": label,
        "batch_id": result.batch_id,
        "orders": orders,
        "config": asdict(config),
        "tables": result.tables,
        "row_count": result.row_count,
        "jsonl_bytes": jsonl_bytes,
        "generate_seconds": elapsed,
        "peak_traced_python_bytes": peak,
        "checksum_replay": "verified",
    }


def profile_fixture_scale(
    output_root: Path, *, small_orders: int = 1_000, large_orders: int = 10_000,
) -> dict[str, Any]:
    if small_orders < 100 or large_orders <= small_orders:
        raise ValueError("order sizes must satisfy 100 <= small < large")
    if output_root.exists() and (not output_root.is_dir() or any(output_root.iterdir())):
        raise ValueError("scale profile requires an empty output directory")
    profiles = [
        _profile(output_root, "small", small_orders),
        _profile(output_root, "large", large_orders),
    ]
    if profiles[1]["jsonl_bytes"] <= profiles[0]["jsonl_bytes"]:
        raise ValueError("larger fixture did not increase JSONL bytes")
    return {
        "status": "ready",
        "scope": "fixture_generation_only",
        "python_version": platform.python_version(),
        "platform": platform.system(),
        "profiles": profiles,
        "jsonl_growth_ratio": profiles[1]["jsonl_bytes"] / profiles[0]["jsonl_bytes"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--small-orders", type=int, default=1_000)
    parser.add_argument("--large-orders", type=int, default=10_000)
    args = parser.parse_args(argv)
    try:
        report = profile_fixture_scale(
            args.output_root, small_orders=args.small_orders, large_orders=args.large_orders,
        )
    except (OSError, ValueError) as error:
        print(f"commerce fixture profile failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
