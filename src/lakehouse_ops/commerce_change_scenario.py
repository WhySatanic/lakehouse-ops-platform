"""Generate a bounded three-snapshot commerce correction acceptance scenario."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any


def _json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _scenario_files() -> tuple[dict[str, bytes], list[dict[str, str]]]:
    files: dict[str, bytes] = {}
    batches = []
    for day, label, total, name in (
        (1, "initial", 1000, "Alice"),
        (2, "correction", 1500, "Alice Updated"),
        (3, "late-arrival", 1500, "Alice Updated"),
    ):
        batch_at = f"2026-02-0{day}T00:00:00Z"
        orders = [
            {
                "order_id": "O0000000001",
                "customer_id": "C00000001",
                "product_id": "P000001",
                "quantity": 1,
                "unit_price_cents": total,
                "total_cents": total,
                "event_at": "2026-01-31T12:00:00Z",
                "ingested_at": batch_at,
            }
        ]
        if day == 3:
            orders.append(
                {
                    "order_id": "O0000000002",
                    "customer_id": "C00000001",
                    "product_id": "P000001",
                    "quantity": 1,
                    "unit_price_cents": 700,
                    "total_cents": 700,
                    "event_at": "2026-01-01T12:00:00Z",
                    "ingested_at": batch_at,
                }
            )
        rows = {
            "customers": [
                {
                    "customer_id": "C00000001",
                    "full_name": name,
                    "email": "alice@example.test",
                    "registered_at": "2025-01-01T00:00:00Z",
                }
            ],
            "products": [
                {
                    "product_id": "P000001",
                    "name": "Training product",
                    "category": "training",
                    "unit_price_cents": total,
                }
            ],
            "orders": orders,
            "payments": [
                {
                    "payment_id": f"PAY{index:010d}",
                    "order_id": order["order_id"],
                    "amount_cents": order["total_cents"],
                    "status": "captured",
                    "paid_at": batch_at,
                }
                for index, order in enumerate(orders, start=1)
            ],
        }
        bodies = {table: b"".join(_json(row) for row in records) for table, records in rows.items()}
        batch_id = hashlib.sha256(
            _json(
                {
                    "scenario": "commerce-snapshots-v1",
                    "batch_at": batch_at,
                    "tables": {
                        table: hashlib.sha256(body).hexdigest() for table, body in bodies.items()
                    },
                }
            )
        ).hexdigest()[:16]
        directory = f"batch_id={batch_id}"
        for table, body in bodies.items():
            files[f"{directory}/{table}.jsonl"] = body
        files[f"{directory}/manifest.json"] = _json(
            {
                "schema_version": 1,
                "batch_id": batch_id,
                "config": {"batch_at": batch_at, "source_semantics": "full_snapshot"},
                "tables": {
                    table: {
                        "file": f"{table}.jsonl",
                        "rows": len(rows[table]),
                        "sha256": hashlib.sha256(body).hexdigest(),
                    }
                    for table, body in bodies.items()
                },
                "quality_cases": {
                    "null_customer_emails": 0,
                    "duplicate_order_rows": 0,
                    "late_orders": int(day == 3),
                    "invalid_payment_amounts": 0,
                },
            }
        )
        batches.append({"label": label, "batch_id": batch_id, "batch_at": batch_at})
    # Deliberately literal acceptance values, independent of the fixture's aggregation.
    files["expected.json"] = _json(
        {
            "schema_version": 1,
            "source_semantics": "full_snapshot",
            "scope": "acceptance_fixture_only",
            "batches": batches,
            "snapshot_revenue_cents": [1000, 1500, 2200],
            "latest_orders": [
                {"order_id": "O0000000001", "total_cents": 1500},
                {"order_id": "O0000000002", "total_cents": 700},
            ],
            "latest_daily_revenue_cents": {"2026-01-01": 700, "2026-01-31": 1500},
            "customer_history": [
                {
                    "full_name": "Alice",
                    "valid_from": "2026-02-01T00:00:00Z",
                    "valid_to": "2026-02-02T00:00:00Z",
                },
                {
                    "full_name": "Alice Updated",
                    "valid_from": "2026-02-02T00:00:00Z",
                    "valid_to": None,
                },
            ],
        }
    )
    return files, batches


def generate_change_scenario(output: Path) -> dict[str, Any]:
    """Publish the complete scenario, or verify an identical existing copy."""
    files, batches = _scenario_files()
    if output.is_symlink():
        raise ValueError("scenario output must not be a symlink")
    if output.exists():
        if not output.is_dir():
            raise ValueError("scenario output must be a directory")
        observed = {
            path.relative_to(output).as_posix()
            for path in output.rglob("*")
            if path.is_file() or path.is_symlink()
        }
        if observed != set(files) or any(
            (output / name).is_symlink() or (output / name).read_bytes() != body
            for name, body in files.items()
        ):
            raise ValueError("existing scenario differs; preserve it and use a new output path")
        created = False
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix=".commerce-scenario-", dir=output.parent))
        try:
            for name, body in files.items():
                target = temporary / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(body)
            os.rename(temporary, output)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)
        created = True
    return {
        "created": created,
        "scope": "acceptance_fixture_only",
        "expected": str(output / "expected.json"),
        "batches": [
            {**batch, "path": str(output / f"batch_id={batch['batch_id']}")} for batch in batches
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        report = generate_change_scenario(args.output)
    except (OSError, ValueError) as error:
        print(f"commerce change scenario failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
