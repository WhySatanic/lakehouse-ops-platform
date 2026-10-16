"""One bounded changing-customer experiment; raw files remain diagnostic on failure."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from lakehouse_ops.commerce_linked_demo import SERVICES, free_ports, release_demo_network

OBSERVER_SETUP = '''
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
policy=/policies/warehouse-inventory-policy.json
sed "s/__BUCKET__/$LAKEHOUSE_BUCKET/g" "$policy" > /tmp/inventory.json
mc admin policy create local lakehouse-warehouse-inventory /tmp/inventory.json
mc admin user add local lakeops-inventory inventory-development-only
mc admin policy attach local lakehouse-warehouse-inventory --user lakeops-inventory
'''


def history_workload(batches):
    if type(batches) is not int or batches not in (4, 50):
        raise ValueError("customer history experiment requires 4 or 50 snapshots")
    return ([0, 1, 1, 2], 1800) if batches == 4 else (list(range(50)), 10800)


def measurements(profile, *, batches=4):
    revisions, _ = history_workload(batches)
    totals = []
    periods = 0
    for index, revision in enumerate(revisions):
        periods += index == 0 or revision != revisions[index - 1]
        totals.append(periods * 10000)
    if profile.get("status") != "ready" or len(profile.get("samples", [])) != batches:
        raise ValueError(f"requires {batches} completed profile samples")
    result = []
    for sample, total in zip(profile["samples"], totals, strict=True):
        history, storage = sample["customer_history"], sample["warehouse_storage"]
        accounting = storage["reference_accounting"]
        if (history["status"] != "verified" or history["total_rows"] != total
                or history["current_rows"] != 10000 or sample["idle_replay"] != "verified"
                or accounting["status"] != "reconciled"
                or accounting["total_payload_bytes"] != storage["total_payload_bytes"]
                or sum(group["bytes"] for group in accounting["groups"].values())
                   != storage["total_payload_bytes"]):
            raise ValueError("history or retained storage oracle mismatch")
        result.append({"batch_id": sample["batch_id"], "history_rows": total,
                       "current_customers": history["current_rows"],
                       "runner_seconds": sample["run"]["durations_seconds"]["total"],
                       "stack_peak_bytes": sample["stack_memory"]["peak_observed_bytes"],
                       "warehouse_payload_bytes": storage["total_payload_bytes"],
                       "groups": accounting["groups"]})
    return result


def experiment(root, output, run=subprocess.run, *, batches=4):
    revisions, profile_timeout = history_workload(batches)
    root, output = root.resolve(), output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    project = "lakehouse-ops-scale-history-" + uuid.uuid4().hex
    env = dict(os.environ)
    env.pop("AWS_SESSION_TOKEN", None)
    env.pop("COMPOSE_PROFILES", None)
    env.update(free_ports(), COMPOSE_PROJECT_NAME=project, COMPOSE_PATH_SEPARATOR=os.pathsep,
               COMPOSE_FILE=os.pathsep.join(str(root / name) for name in (
                   "compose.yaml", "compose.spark-memory.yaml")),
               COMMERCE_STAGE_TIMEOUT_SECONDS="300", LAKEHOUSE_BUCKET="lakehouse",
               AWS_ACCESS_KEY_ID="lakeops-ingest",
               AWS_SECRET_ACCESS_KEY="ingest-development-only",
               MINIO_INGEST_USER="lakeops-ingest", MINIO_INGEST_PASSWORD="ingest-development-only",
               WAREHOUSE_INVENTORY_ACCESS_KEY_ID="lakeops-inventory",
               WAREHOUSE_INVENTORY_SECRET_ACCESS_KEY="inventory-development-only")
    compose = ["docker", "compose", "--project-name", project,
               "--profile", "catalog", "--profile", "compute", "--profile", "query"]
    report = {"status": "failed", "source_sha": os.getenv("GITHUB_SHA"), "project": project,
              "scope": "one_fresh_stack_bounded_snapshots_not_capacity_or_billing",
              "workload": {"customers": 10000, "orders": 100, "batches": batches,
                           "customer_revisions": revisions}, "steps": []}

    def execute(label, command, timeout):
        step = {"name": label, "status": "failed", "timeout_seconds": timeout}
        report["steps"].append(step)
        started = time.perf_counter()
        try:
            with (output / f"{label}.log").open("wb") as log:
                completed = run(command, cwd=root, env=dict(env), stdout=log,
                                stderr=subprocess.STDOUT, check=True, timeout=timeout)
            step.update(status="ready", returncode=completed.returncode)
        finally:
            step["elapsed_seconds"] = time.perf_counter() - started

    try:
        execute("start", [*compose, "up", "-d", "--wait", "--wait-timeout", "180",
                          "--no-build", "--pull", "never", *SERVICES], 240)
        execute("observer", [*compose, "run", "--rm", "--no-deps", "--entrypoint", "/bin/sh",
                             "minio-init", "-ec", OBSERVER_SETUP], 60)
        execute("profile", [sys.executable, "-m", "lakehouse_ops.commerce_pipeline_profile",
                            "--output", str(output / "profile"), "--orders", "100", "100",
                            "--customers", "10000", "--batches", str(batches),
                            "--customer-revisions", *map(str, revisions),
                            "--warehouse-inventory", "--retained-references",
                            "--s3-endpoint-url", f"http://localhost:{env['MINIO_API_PORT']}",
                            "--server", f"http://localhost:{env['TRINO_HTTP_PORT']}"],
                profile_timeout)
        raw = (output / "profile/profile.json").read_bytes()
        report["measurements"] = measurements(json.loads(raw), batches=batches)
        report["profile_sha256"] = hashlib.sha256(raw).hexdigest()
        report["status"] = "ready"
    except Exception as error:
        report.update(error_type=type(error).__name__, error=str(error))
        raise
    finally:
        try:
            execute("services", [*compose, "logs", "--no-color"], 30)
        except (OSError, subprocess.SubprocessError) as error:
            report["logs_error"] = str(error)
        try:
            execute("stop", [*compose, "down"], 120)
            report["network_cleanup"] = release_demo_network(project, root, env, run)
        except (OSError, subprocess.SubprocessError, ValueError, RuntimeError) as error:
            report.update(status="failed", cleanup_error=str(error))
        (output / "experiment.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


@pytest.mark.skipif(os.getenv("LAKEOPS_RUN_CUSTOMER_HISTORY_EXPERIMENT") != "1",
                    reason="requires prebuilt images and one owned Docker experiment stack")
def test_changing_customer_snapshots_and_retained_warehouse():
    batches = int(os.getenv("LAKEOPS_CUSTOMER_HISTORY_BATCHES", "4"))
    report = experiment(Path.cwd(), Path("artifacts/customer-history-ci"), batches=batches)
    assert report["status"] == "ready", report.get("cleanup_error")
