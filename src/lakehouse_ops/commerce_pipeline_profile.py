"""Two-volume local commerce execution evidence on an already started isolated stack."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from lakehouse_ops.ingestion.commerce_fixture import (
    CommerceFixtureConfig,
    generate_commerce_fixture,
)
from lakehouse_ops.trino import TrinoClient


def memory_reports(log: str) -> list[dict[str, Any]]:
    reports = []
    for line in log.splitlines():
        if not line.startswith('{"app_name":'):
            continue
        report = json.loads(line)
        if report.get("kind") == "spark_container_memory":
            reports.append(report)
    return reports


def execute(args: list[str], output: Path, label: str) -> dict[str, Any]:
    with (output / f"{label}.stdout.json").open("w") as stdout, (
        output / f"{label}.stderr.log"
    ).open("w") as stderr:
        subprocess.run(
            [sys.executable, "-m", "lakehouse_ops.cli", *args],
            stdout=stdout, stderr=stderr, check=True, timeout=1800,
        )
    return json.loads((output / f"{label}.stdout.json").read_text())


def file_inventory(client: TrinoClient) -> list[dict[str, Any]]:
    tables = client.query(
        "SELECT table_schema, table_name FROM lakehouse.information_schema.tables "
        "WHERE table_schema IN ('bronze','silver','gold') "
        "AND table_name LIKE 'commerce_%' ORDER BY table_schema, table_name"
    )
    inventory = []
    for table in tables:
        schema, name = table["table_schema"], table["table_name"]
        # Identifiers originate in catalog metadata; quote rather than interpolate raw SQL.
        schema = schema.replace('"', '""')
        name = name.replace('"', '""')
        identifier = f'"{schema}"."{name}$files"'
        values = client.query(
            f"SELECT count(*) AS files, coalesce(sum(file_size_in_bytes),0) AS bytes "
            f"FROM lakehouse.{identifier}"
        )[0]
        inventory.append({"table": f"{schema}.{name}", **values})
    return inventory


def query_costs(client: TrinoClient, batch_id: str, orders: int) -> dict[str, Any]:
    """Compare a batch-scoped detail aggregation with its materialized daily mart."""
    if re.fullmatch(r"[0-9a-f]{16}", batch_id) is None:
        raise ValueError("requires a fixture batch ID")
    predicate = f"source_batch_id='{batch_id}'"
    detail = client.query_with_stats(
        "WITH payments AS (SELECT order_id, "
        "sum(CASE WHEN status='captured' THEN amount_cents ELSE 0 END) AS revenue "
        f"FROM lakehouse.silver.commerce_payments WHERE {predicate} GROUP BY order_id) "
        "SELECT count(*) AS orders, coalesce(sum(p.revenue),0) AS revenue "
        "FROM lakehouse.silver.commerce_orders o LEFT JOIN payments p "
        f"ON o.order_id=p.order_id WHERE o.{predicate}"
    )
    daily = client.query_with_stats(
        "SELECT sum(order_count) AS orders, sum(captured_revenue_cents) AS revenue "
        f"FROM lakehouse.gold.commerce_daily WHERE {predicate}"
    )
    if (len(detail.rows) != 1 or len(daily.rows) != 1 or detail.rows != daily.rows
            or detail.rows[0]["orders"] != orders):
        raise ValueError("detail and daily query results do not reconcile to selected volume")
    return {
        "scope": "single_detail_then_daily_query_not_capacity",
        "detail": asdict(detail), "daily": asdict(daily),
    }


def profile(output: Path, *, orders: list[int], endpoint: str, server: str) -> dict[str, Any]:
    if len(orders) != 2 or not 100 <= orders[0] < orders[1]:
        raise ValueError("requires two ascending order volumes >= 100")
    project = os.environ.get("COMPOSE_PROJECT_NAME", "")
    if project != "lakehouse-ops-scale" and not project.startswith("lakehouse-ops-scale-"):
        raise ValueError("requires a dedicated COMPOSE_PROJECT_NAME=lakehouse-ops-scale-<name>")
    if output.exists() and any(output.iterdir()):
        raise ValueError("requires empty output directory")
    output.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "status": "running", "scope": "two_volume_sequential_warm_stack",
        "project": project, "samples": [], "recovery": "idle_checkpoint_replay_only",
        "limitations": ["not isolated per-volume capacity", "no crash recovery injection",
                        "memory excludes HMS/MinIO/Trino and remote executors"],
    }
    try:
        with TrinoClient(server, user="lakehouse-performance") as client:
            if file_inventory(client):
                raise ValueError("requires fresh commerce catalog in the dedicated stack")
            for index, volume in enumerate(orders):
                label = f"sample-{index + 1}"
                config = CommerceFixtureConfig(
                    customers=10, products=5, orders=volume, null_customer_emails=1,
                    duplicate_orders=1, late_orders=1, invalid_payments=1,
                    batch_at=f"2026-02-0{index + 1}T00:00:00+00:00",
                )
                fixture = generate_commerce_fixture(output / label, config)
                execute([
                    "land-commerce-fixture", "--fixture", str(fixture.path),
                    "--s3-endpoint-url", endpoint, "--s3-bucket", "lakehouse",
                ], output, f"{label}-landing")
                command = [
                    "run-commerce-batch", "--state", str(output / "checkpoint.json"),
                    "--s3-endpoint-url", endpoint, "--server", server,
                    "--s3-bucket", "lakehouse",
                    "--user", "lakehouse-performance", "--attempts", "3",
                ]
                run = execute(command, output, label)
                if run.get("status") != "ready" or run.get("batch_id") != fixture.batch_id:
                    raise ValueError("runner did not verify the selected volume")
                memory = memory_reports(
                    (output / f"{label}.stderr.log").read_text(encoding="utf-8")
                )
                if len(memory) != 7 or any(r.get("status") != "observed" for r in memory):
                    raise ValueError("requires observed memory for all seven Spark stages")
                files = file_inventory(client)
                query = client.query_with_stats(
                    "SELECT sum(captured_revenue_cents) AS revenue "
                    "FROM lakehouse.gold.commerce_daily "
                    f"WHERE source_batch_id='{fixture.batch_id}'"
                )
                costs = query_costs(client, fixture.batch_id, volume)
                idle = execute(command, output, f"{label}-idle")
                if idle.get("status") != "idle" or file_inventory(client) != files:
                    raise ValueError("idle checkpoint replay changed the file inventory")
                report["samples"].append({
                    "orders": volume, "batch_id": fixture.batch_id, "config": asdict(config),
                    "run": run, "memory": memory, "cumulative_current_files": files,
                    "query_id": query.query_id, "query_stats": asdict(query.stats),
                    "query_rows": query.rows, "idle_replay": "verified",
                    "query_costs": costs,
                })
                (output / "profile.json").write_text(json.dumps(report, indent=2))
        report["status"] = "ready"
    except Exception as error:
        report["status"] = "failed"
        report["error_type"] = type(error).__name__
        raise
    finally:
        (output / "profile.json").write_text(json.dumps(report, indent=2))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--orders", nargs=2, type=int, default=[100, 1000])
    parser.add_argument("--s3-endpoint-url", required=True)
    parser.add_argument("--server", required=True)
    args = parser.parse_args()
    try:
        report = profile(args.output, orders=args.orders, endpoint=args.s3_endpoint_url,
                         server=args.server)
    except Exception as error:
        print(f"commerce pipeline profile failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
