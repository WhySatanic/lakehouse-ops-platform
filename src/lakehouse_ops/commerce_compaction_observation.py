"""Opt-in real compaction evidence on a diagnostic copy in an owned scale stack."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
from dataclasses import asdict
from pathlib import Path

from lakehouse_ops.commerce_runner_lock import commerce_runner_lock
from lakehouse_ops.iceberg.metadata import IcebergMetadataCollector
from lakehouse_ops.iceberg.planner import IcebergMaintenancePlanner, MaintenancePolicy
from lakehouse_ops.trino import TrinoClient

SOURCE = "commerce_daily"
TARGET = "commerce_compaction_daily"


def capture(client, table):
    if table not in {SOURCE, TARGET}:
        raise ValueError("unsupported observation table")
    rows = client.query(f"SELECT * FROM lakehouse.gold.{table}")
    digest = hashlib.sha256("\n".join(sorted(
        json.dumps(row, sort_keys=True, default=str) for row in rows
    )).encode()).hexdigest()
    files = client.query(
        f'SELECT count(*) AS files, sum(file_size_in_bytes) AS bytes '
        f'FROM lakehouse.gold."{table}$files" WHERE content=0'
    )[0]
    snapshot = client.query(
        f'SELECT snapshot_id FROM lakehouse.gold."{table}$history" '
        "ORDER BY made_current_at DESC LIMIT 1"
    )[0]["snapshot_id"]
    query = client.query_with_stats(
        f"SELECT sum(order_count) AS orders, sum(captured_revenue_cents) AS revenue "
        f"FROM lakehouse.gold.{table}"
    )
    return {"rows": len(rows), "rows_sha256": digest, "files": files,
            "snapshot_id": str(snapshot), "query": asdict(query)}


def observe(client, stage, output, state, *, clock=time.perf_counter):
    checkpoint = state.read_bytes()
    report = {"status": "failed", "scope": "diagnostic_copy_compaction_not_production_capacity"}
    try:
        source = capture(client, SOURCE)
        report["source_before"] = source
        started = clock()
        stage("prepare", None)
        report["prepare_seconds"] = clock() - started
        before = capture(client, TARGET)
        report["before"] = before
        if (before["rows_sha256"] != source["rows_sha256"] or before["rows"] != source["rows"]
                or before["files"]["files"] < 2):
            raise ValueError("diagnostic copy does not match source or is not fragmented")
        started = clock()
        stage("compact", before["snapshot_id"])
        report["compaction_seconds"] = clock() - started
        after = capture(client, TARGET)
        report["after"] = after
        if (before["rows_sha256"] != after["rows_sha256"] or before["rows"] != after["rows"]
                or before["query"]["rows"] != after["query"]["rows"]
                or after["files"]["files"] >= before["files"]["files"]
                or after["snapshot_id"] == before["snapshot_id"]):
            raise ValueError("compaction changed rows/revenue or did not reduce files")
        source_after = capture(client, SOURCE)
        report["source_after"] = source_after
        if any(source[key] != source_after[key] for key in
               ("rows", "rows_sha256", "files", "snapshot_id")) or state.read_bytes() != checkpoint:
            raise ValueError("source gold or processing checkpoint changed")
        report.update(status="ready", checkpoint="unchanged", source="unchanged")
    except Exception as error:
        report["error_type"] = type(error).__name__
        raise
    finally:
        (output / "compaction.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--server", required=True)
    parser.add_argument("--confirm-diagnostic-compaction", action="store_true")
    args = parser.parse_args()
    project = os.environ.get("COMPOSE_PROJECT_NAME", "")
    if not args.confirm_diagnostic_compaction or not re.fullmatch(
        r"lakehouse-ops-scale-demo-[0-9a-f]{32}", project,
    ):
        parser.error("requires confirmation and an explicit owned scale-demo project")
    root = Path.cwd().resolve()
    output = args.output.resolve()
    try:
        relative = output.relative_to(root / "artifacts")
    except ValueError:
        parser.error("output must be inside this checkout's artifacts directory")
    if output.exists() and any(output.iterdir()):
        parser.error("requires an empty output directory")
    if not args.state.is_file():
        parser.error("requires the retained processing checkpoint")
    output.mkdir(parents=True, exist_ok=True)

    def stage(mode, snapshot):
        if mode == "compact":
            metadata = IcebergMetadataCollector(client).collect("lakehouse", "gold", TARGET)
            if metadata.snapshots.current_id != snapshot:
                raise ValueError("diagnostic snapshot changed before planning")
            plan = IcebergMaintenancePlanner(MaintenancePolicy(min_data_files=2)).plan(
                metadata.as_dict(),
            ).as_dict()
            (output / "plan.json").write_text(json.dumps(plan, indent=2), encoding="utf-8")
        container_path = f"/opt/lakehouse/artifacts/{relative.as_posix()}/{mode}.json"
        plan_path = f"/opt/lakehouse/artifacts/{relative.as_posix()}/plan.json"
        command = ["docker", "compose", "--project-name", project, "--profile", "compute",
                   "run", "--rm", "--no-deps", "-e", "COMPACTION_CONFIRMED=1",
                   "-e", f"COMPACTION_MODE={mode}",
                   "-e", f"COMPACTION_REPORT_PATH={container_path}",
                   "-e", f"COMPACTION_PLAN_PATH={plan_path}",
                   "-e", f"COMPACTION_EXPECTED_SNAPSHOT={snapshot or ''}", "spark-maintenance",
                   "/opt/spark/bin/spark-submit",
                   "/opt/lakehouse/jobs/commerce_compaction_observation.py"]
        with (output / f"{mode}.log").open("wb") as log:
            subprocess.run(
                command, check=True, stdout=log, stderr=subprocess.STDOUT, timeout=300,
                env={**os.environ, "COMPOSE_FILE": str(root / "compose.yaml"),
                     "COMPOSE_PROJECT_NAME": project},
            )
        if json.loads((output / f"{mode}.json").read_text())["status"] != "ready":
            raise ValueError("Spark observation did not retain a ready report")

    try:
        with (commerce_runner_lock(),
              TrinoClient(args.server, user="lakehouse-performance", query_timeout=60) as client):
            report = observe(client, stage, output, args.state)
    except Exception as error:
        print(f"commerce compaction observation failed: {type(error).__name__}")
        return 2
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
