from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
import tempfile
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


class CommerceFixtureError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CommerceFixtureConfig:
    customers: int = 10_000
    products: int = 1_000
    orders: int = 100_000
    null_customer_emails: int = 1_000
    duplicate_orders: int = 1_000
    late_orders: int = 1_000
    invalid_payments: int = 1_000
    seed: int = 20260921
    batch_at: str = "2026-01-31T00:00:00+00:00"

    def __post_init__(self) -> None:
        if min(self.customers, self.products, self.orders) < 1:
            raise CommerceFixtureError("customers, products, and orders must be positive")
        limits = {
            "null_customer_emails": (self.null_customer_emails, self.customers),
            "duplicate_orders": (self.duplicate_orders, self.orders),
            "late_orders": (self.late_orders, self.orders),
            "invalid_payments": (self.invalid_payments, self.orders),
        }
        for name, (value, maximum) in limits.items():
            if not 0 <= value <= maximum:
                raise CommerceFixtureError(f"{name} must be between 0 and {maximum}")
        if self.late_orders + self.duplicate_orders > self.orders:
            raise CommerceFixtureError(
                "late_orders and duplicate_orders must fit in distinct canonical orders"
            )
        try:
            batch_at = datetime.fromisoformat(self.batch_at)
        except ValueError as error:
            raise CommerceFixtureError("batch_at must be an ISO-8601 timestamp") from error
        if batch_at.tzinfo is None:
            raise CommerceFixtureError("batch_at must include a timezone")


@dataclass(frozen=True, slots=True)
class CommerceFixtureResult:
    path: Path
    batch_id: str
    created: bool
    row_count: int
    tables: dict[str, int]

    def as_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "created": self.created,
            "path": str(self.path),
            "row_count": self.row_count,
            "tables": self.tables,
        }


def generate_commerce_fixture(
    output: Path, config: CommerceFixtureConfig
) -> CommerceFixtureResult:
    normalized = _normalized_config(config)
    batch_id = hashlib.sha256(_canonical_json(normalized)).hexdigest()[:16]
    destination = output / f"batch_id={batch_id}"
    if destination.exists():
        manifest = _verify_existing(destination, normalized, batch_id)
        return _result(destination, batch_id, False, manifest)

    output.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{batch_id}.", dir=output))
    try:
        table_rows = _write_tables(temporary, config)
        tables = {
            name: {
                "file": f"{name}.jsonl",
                "rows": rows,
                "sha256": _file_sha256(temporary / f"{name}.jsonl"),
            }
            for name, rows in table_rows.items()
        }
        manifest = {
            "schema_version": 1,
            "batch_id": batch_id,
            "config": normalized,
            "tables": tables,
            "quality_cases": {
                "null_customer_emails": config.null_customer_emails,
                "duplicate_order_rows": config.duplicate_orders,
                "late_orders": config.late_orders,
                "invalid_payment_amounts": config.invalid_payments,
            },
        }
        _write_json(temporary / "manifest.json", manifest)
        try:
            os.replace(temporary, destination)
        except FileExistsError:
            shutil.rmtree(temporary)
            manifest = _verify_existing(destination, normalized, batch_id)
            return _result(destination, batch_id, False, manifest)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return _result(destination, batch_id, True, manifest)


def _write_tables(root: Path, config: CommerceFixtureConfig) -> dict[str, int]:
    random_source = random.Random(config.seed)
    batch_at = datetime.fromisoformat(config.batch_at).astimezone(UTC)
    customers = _customers(config)
    products = _products(config)
    orders = _orders(config, products, batch_at, random_source)
    payments = _payments(config, orders)
    rows = {
        "customers": customers,
        "products": products,
        "orders": [
            *orders,
            *orders[
                config.late_orders : config.late_orders + config.duplicate_orders
            ],
        ],
        "payments": payments,
    }
    for table, records in rows.items():
        _write_json_lines(root / f"{table}.jsonl", records)
    return {table: len(records) for table, records in rows.items()}


def _customers(config: CommerceFixtureConfig) -> list[dict[str, Any]]:
    return [
        {
            "customer_id": f"C{index:08d}",
            "full_name": f"Customer {index:08d}",
            "email": None
            if index <= config.null_customer_emails
            else f"customer{index:08d}@example.test",
            "registered_at": f"2025-01-{((index - 1) % 28) + 1:02d}T00:00:00Z",
        }
        for index in range(1, config.customers + 1)
    ]


def _products(config: CommerceFixtureConfig) -> list[dict[str, Any]]:
    return [
        {
            "product_id": f"P{index:06d}",
            "name": f"Product {index:06d}",
            "category": f"category_{((index - 1) % 12) + 1:02d}",
            "unit_price_cents": 500 + ((index * 137) % 49_500),
        }
        for index in range(1, config.products + 1)
    ]


def _orders(
    config: CommerceFixtureConfig,
    products: list[dict[str, Any]],
    batch_at: datetime,
    random_source: random.Random,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for index in range(1, config.orders + 1):
        product_index = random_source.randrange(config.products)
        quantity = random_source.randint(1, 5)
        age_days = 45 + (index % 15) if index <= config.late_orders else index % 30
        event_at = batch_at - timedelta(days=age_days, seconds=index % 86_400)
        unit_price = products[product_index]["unit_price_cents"]
        records.append(
            {
                "order_id": f"O{index:010d}",
                "customer_id": f"C{random_source.randrange(1, config.customers + 1):08d}",
                "product_id": products[product_index]["product_id"],
                "quantity": quantity,
                "unit_price_cents": unit_price,
                "total_cents": quantity * unit_price,
                "event_at": event_at.isoformat().replace("+00:00", "Z"),
                "ingested_at": batch_at.isoformat().replace("+00:00", "Z"),
            }
        )
    return records


def _payments(
    config: CommerceFixtureConfig, orders: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    return [
        {
            "payment_id": f"PAY{index:010d}",
            "order_id": order["order_id"],
            "amount_cents": None if index <= config.invalid_payments else order["total_cents"],
            "status": "captured",
            "paid_at": order["ingested_at"],
        }
        for index, order in enumerate(orders, start=1)
    ]


def _normalized_config(config: CommerceFixtureConfig) -> dict[str, Any]:
    normalized = asdict(config)
    normalized["batch_at"] = (
        datetime.fromisoformat(config.batch_at)
        .astimezone(UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )
    return normalized


def _verify_existing(
    destination: Path, expected_config: dict[str, Any], batch_id: str
) -> dict[str, Any]:
    try:
        manifest = json.loads((destination / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CommerceFixtureError(f"existing fixture manifest is unreadable: {error}") from error
    if manifest.get("batch_id") != batch_id or manifest.get("config") != expected_config:
        raise CommerceFixtureError("existing fixture does not match the requested configuration")
    for table in manifest.get("tables", {}).values():
        path = destination / table["file"]
        if not path.is_file() or _file_sha256(path) != table["sha256"]:
            raise CommerceFixtureError(f"existing fixture failed checksum verification: {path}")
    return manifest


def _result(
    path: Path, batch_id: str, created: bool, manifest: dict[str, Any]
) -> CommerceFixtureResult:
    tables = {name: details["rows"] for name, details in manifest["tables"].items()}
    return CommerceFixtureResult(path, batch_id, created, sum(tables.values()), tables)


def _write_json_lines(path: Path, records: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for record in records:
            stream.write(_canonical_json(record).decode("utf-8"))
            stream.write("\n")


def _write_json(path: Path, document: dict[str, Any]) -> None:
    path.write_bytes(_canonical_json(document) + b"\n")


def _canonical_json(document: dict[str, Any]) -> bytes:
    return json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
