"""Two-volume local commerce execution evidence on an already started isolated stack."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from threading import Event, Thread
from typing import Any

from lakehouse_ops.commerce_pipeline import (
    COMMERCE_RUN_CLEANUP_GRACE_SECONDS,
    COMMERCE_STAGES,
    CURRENT_STAGE,
    commerce_run_timeout_seconds,
)
from lakehouse_ops.ingestion.commerce_fixture import (
    CommerceFixtureConfig,
    generate_commerce_fixture,
)
from lakehouse_ops.trino import TrinoClient

_INVENTORY_TABLES_SQL = (
    "SELECT table_schema, table_name FROM lakehouse.information_schema.tables "
    "WHERE table_schema IN ('bronze','silver','gold') "
    "AND (table_name LIKE 'commerce_%' "
    "OR (table_schema='gold' AND table_name='dim_customers_scd2')) "
    "ORDER BY table_schema, table_name"
)


def stack_memory_sample(project: str) -> dict[str, Any]:
    """Observe only this Compose project's containers, including active one-off jobs."""
    if project != "lakehouse-ops-scale" and not project.startswith("lakehouse-ops-scale-"):
        raise ValueError("stack memory requires a dedicated scale project")
    def discover():
        return subprocess.run(
            ["docker", "ps", "--filter", f"label=com.docker.compose.project={project}",
             "--format", "{{.ID}}"], check=True, capture_output=True, text=True, timeout=10,
        ).stdout.split()

    ids = discover()
    if not ids:
        raise ValueError("no running containers in the selected project")
    retired = None
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
            check=True, capture_output=True, text=True, timeout=10,
        )
    except subprocess.CalledProcessError as error:
        stderr = (error.stderr or "").strip()
        missing = re.fullmatch(
            r"Error response from daemon: No such container: ([0-9a-f]{12,64})",
            stderr,
        )
        # Docker may close an in-flight stats stream with EOF as an --rm job exits.
        # EOF alone is not proof of retirement: check the same owned project again.
        if (missing is None and stderr != "EOF") or (missing and missing[1] not in ids):
            raise
        fresh = discover()
        gone = set(ids) - set(fresh)
        if not fresh or len(gone) != 1 or (missing and gone != {missing[1]}):
            raise
        retired, ids = gone.pop(), fresh
        # One retry only, on newly discovered owned IDs, never all-host stats.
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
            check=True, capture_output=True, text=True, timeout=10,
        )
    containers = []
    for line in result.stdout.splitlines():
        row = json.loads(line)
        used = row["MemUsage"].split("/")[0].strip()
        match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(B|KiB|MiB|GiB|kB|MB|GB)", used)
        if not match:
            raise ValueError("unrecognized Docker memory unit")
        factors = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3,
                   "kB": 1000, "MB": 1000**2, "GB": 1000**3}
        containers.append({"id": row["ID"], "name": row["Name"],
                           "used_bytes": round(float(match[1]) * factors[match[2]])})
    if len(containers) != len(ids) or {row["id"] for row in containers} != set(ids):
        raise ValueError("project containers changed during memory sampling")
    sample = {"containers": containers,
              "used_bytes": sum(row["used_bytes"] for row in containers)}
    if retired is not None:
        sample["resampled_after_container_exit"] = retired
    return sample


class StackMemorySampler:
    """Sample the whole project during the existing runner, without controlling it."""

    def __init__(self, project: str):
        self.project = project
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.stop = Event()
        self.thread = Thread(target=self._observe, daemon=True)
        self.started = time.perf_counter()

    def _sample(self):
        try:
            sample = stack_memory_sample(self.project)
            self.samples.append({"elapsed_seconds": time.perf_counter() - self.started, **sample})
        except (OSError, subprocess.SubprocessError, ValueError, KeyError) as error:
            self.errors.append(f"{type(error).__name__}: {error}")

    def _observe(self):
        while not self.stop.wait(2):
            self._sample()

    def __enter__(self):
        self._sample()
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=45)
        if self.thread.is_alive():
            self.errors.append("memory observer did not finish within its bounded shutdown")

    def report(self):
        return {"status": "observed" if self.samples and not self.errors else "partial",
                "scope": "sampled_project_container_memory_not_host_rss",
                "cache": "Docker CLI excludes inactive file cache on Linux",
                "peak_observed_bytes": max((s["used_bytes"] for s in self.samples), default=None),
                "samples": list(self.samples), "errors": list(self.errors)}


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
    timeout = (commerce_run_timeout_seconds() + COMMERCE_RUN_CLEANUP_GRACE_SECONDS
               if args and args[0] == "run-commerce-batch" else 1800)
    with (output / f"{label}.stdout.json").open("w") as stdout, (
        output / f"{label}.stderr.log"
    ).open("w") as stderr:
        subprocess.run(
            [sys.executable, "-m", "lakehouse_ops.cli", *args],
            stdout=stdout, stderr=stderr, check=True, timeout=timeout,
        )
    return json.loads((output / f"{label}.stdout.json").read_text())


def file_inventory(client: TrinoClient) -> list[dict[str, Any]]:
    tables = client.query(_INVENTORY_TABLES_SQL)
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


def query_costs(
    client: TrinoClient, batch_id: str, orders: int, *, daily_first: bool = False,
) -> dict[str, Any]:
    """Compare a batch-scoped detail aggregation with its materialized daily mart."""
    if re.fullmatch(r"[0-9a-f]{16}", batch_id) is None:
        raise ValueError("requires a fixture batch ID")
    predicate = f"source_batch_id='{batch_id}'"
    detail_sql = (
        "WITH payments AS (SELECT order_id, "
        "sum(CASE WHEN status='captured' THEN amount_cents ELSE 0 END) AS revenue "
        f"FROM lakehouse.silver.commerce_payments WHERE {predicate} GROUP BY order_id) "
        "SELECT count(*) AS orders, coalesce(sum(p.revenue),0) AS revenue "
        "FROM lakehouse.silver.commerce_orders o LEFT JOIN payments p "
        f"ON o.order_id=p.order_id WHERE o.{predicate}"
    )
    daily_sql = (
        "SELECT sum(order_count) AS orders, sum(captured_revenue_cents) AS revenue "
        f"FROM lakehouse.gold.commerce_daily WHERE {predicate}"
    )
    if daily_first:
        daily = client.query_with_stats(daily_sql)
        detail = client.query_with_stats(detail_sql)
    else:
        detail = client.query_with_stats(detail_sql)
        daily = client.query_with_stats(daily_sql)
    if (len(detail.rows) != 1 or len(daily.rows) != 1 or detail.rows != daily.rows
            or detail.rows[0]["orders"] != orders):
        raise ValueError("detail and daily query results do not reconcile to selected volume")
    return {
        "scope": ("single_daily_then_detail_query_not_capacity" if daily_first
                  else "single_detail_then_daily_query_not_capacity"),
        "detail": asdict(detail), "daily": asdict(daily),
    }


def snapshot_inventory(client: TrinoClient) -> list[dict[str, Any]]:
    tables = client.query(_INVENTORY_TABLES_SQL)
    inventory = []
    for table in tables:
        schema = table["table_schema"].replace('"', '""')
        name = table["table_name"].replace('"', '""')
        snapshots = client.query(
            f'SELECT snapshot_id FROM lakehouse."{schema}"."{name}$snapshots" '
            "ORDER BY snapshot_id"
        )
        inventory.append({"table": f"{schema}.{name}",
                          "snapshot_ids": [row["snapshot_id"] for row in snapshots]})
    return inventory


def measure_idle_replay(
    client: TrinoClient, command: list[str], output: Path, label: str, *,
    clock: Callable[[], float] = time.perf_counter,
) -> dict[str, Any]:
    """Time the idle child only; catalog verification is deliberately outside its timer."""
    files = file_inventory(client)
    snapshots = snapshot_inventory(client)
    started = clock()
    idle = execute(command, output, label)
    elapsed = clock() - started
    if (idle.get("status") != "idle" or idle.get("batch_id") is not None
            or idle.get("completed_stages") != []):
        raise ValueError("checkpoint replay did not report a compute-free idle run")
    if file_inventory(client) != files or snapshot_inventory(client) != snapshots:
        raise ValueError("idle checkpoint replay changed files or snapshots")
    return {
        "status": "verified", "elapsed_seconds": elapsed,
        "scope": "single_idle_child_wall_time_including_startup_not_capacity",
        "completed_stages": [], "files": files, "snapshots": snapshots,
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
        "source_semantics": "full_snapshot",
        "limitations": ["not isolated per-volume capacity", "no crash recovery injection",
                        "sampled stack memory excludes host/VM overhead and inactive file cache",
                        "discrete samples can miss short memory peaks"],
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
                fixture = generate_commerce_fixture(output / label, config,
                                                     source_semantics="full_snapshot")
                # Independent of Spark and the SQL marts: read this synthetic input.
                with (fixture.path / "orders.jsonl").open(encoding="utf-8") as stream:
                    order_ids = {json.loads(line)["order_id"] for line in stream}
                with (fixture.path / "payments.jsonl").open(encoding="utf-8") as stream:
                    payments = (json.loads(line) for line in stream)
                    expected_revenue = sum(
                        row["amount_cents"] for row in payments
                        if row["order_id"] in order_ids and row["status"] == "captured"
                        and type(row["amount_cents"]) is int and row["amount_cents"] >= 0
                    )
                expected = {"orders": len(order_ids), "revenue": expected_revenue}
                if expected["orders"] != volume:
                    raise ValueError("input order oracle does not match the requested volume")
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
                stack_memory = StackMemorySampler(project)
                try:
                    with stack_memory:
                        run = execute(command, output, label)
                finally:
                    stack_observation = stack_memory.report()
                    (output / f"{label}.stack-memory.json").write_text(
                        json.dumps(stack_observation, indent=2), encoding="utf-8",
                    )
                if run.get("status") != "ready" or run.get("batch_id") != fixture.batch_id:
                    raise ValueError("runner did not verify the selected volume")
                if run.get("completed_stages") != [*COMMERCE_STAGES, CURRENT_STAGE]:
                    raise ValueError("requires all nine full-snapshot runner stages")
                memory = memory_reports(
                    (output / f"{label}.stderr.log").read_text(encoding="utf-8")
                )
                if len(memory) != 8 or any(r.get("status") != "observed" for r in memory):
                    raise ValueError("requires observed memory for all eight Spark stages")
                files = file_inventory(client)
                query = client.query_with_stats(
                    "SELECT sum(captured_revenue_cents) AS revenue "
                    "FROM lakehouse.gold.commerce_daily "
                    f"WHERE source_batch_id='{fixture.batch_id}'"
                )
                costs = query_costs(client, fixture.batch_id, volume)
                current = client.query_with_stats(
                    "SELECT sum(order_count) AS orders, "
                    "sum(captured_revenue_cents) AS revenue "
                    "FROM lakehouse.gold.commerce_current_daily"
                )
                if (costs["detail"]["rows"] != (expected,)
                        or current.rows != (expected,) or query.rows != (
                            {"revenue": expected_revenue},
                        )):
                    raise ValueError("SQL results do not reconcile to independent input oracle")
                idle = measure_idle_replay(client, command, output, f"{label}-idle")
                report["samples"].append({
                    "orders": volume, "batch_id": fixture.batch_id, "config": asdict(config),
                    "run": run, "memory": memory, "cumulative_current_files": files,
                    "stack_memory": stack_observation,
                    "query_id": query.query_id, "query_stats": asdict(query.stats),
                    "query_rows": query.rows, "idle_replay": "verified",
                    "query_costs": costs,
                    "input_expected": expected, "current_query": asdict(current),
                    "idle_replay_observation": idle,
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
