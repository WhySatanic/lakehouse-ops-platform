"""Synthetic retained reports test audit logic, never live Spark or 100-run success."""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

SOURCE = "6459ed9595b2a387c7ff60f36727484160efedaa"


def module():
    path = Path(__file__).parents[1] / "docs/evidence/audit-customer-history.py"
    spec = importlib.util.spec_from_file_location("history_audit", path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture(scope="module")
def retained(tmp_path_factory):
    base = tmp_path_factory.mktemp("synthetic-history")
    root = base / "customer-history-ci/profile"
    runner = base / "customer-history-runner/source-sha.txt"
    runner.parent.mkdir(parents=True)
    runner.write_text(SOURCE)
    start = datetime(2026, 2, 1, tzinfo=UTC)
    samples, checkpoint = [], {}
    groups = {key: {"versions": 0, "bytes": 0} for key in (
        "current_data", "current_delete", "retained_only_data", "retained_only_delete",
        "latest_not_in_selected_file_references", "s3_noncurrent",
    )}
    groups["current_data"] = {"versions": 1, "bytes": 1024}
    ref = {"file_path": "s3a://lakehouse/warehouse/one.parquet", "bytes": 1024, "content": 0}
    storage = {"bucket": "lakehouse", "total_payload_bytes": 1024,
               "objects": [{"key": "warehouse/one.parquet", "version_id": "v1",
                            "is_latest": True, "bytes": 1024}],
               "reference_accounting": {"status": "reconciled", "orphan_classification":
                   "not_performed", "total_payload_bytes": 1024, "groups": groups,
                   "references": [{"current": [ref], "retained": [ref]}]}}
    for index in range(100):
        n, batch = index + 1, f"synthetic-{index}"
        fixture = root / f"sample-{n}/batch_id={batch}"
        fixture.mkdir(parents=True)
        suffix = f" revision {index}" if index else ""
        tables = {
            "customers": [{"customer_id": f"C{i:08d}",
                           "full_name": f"Customer {i:08d}{suffix}"} for i in range(1, 10001)],
            "orders": [{"order_id": f"O{i}"} for i in range(100)],
            "payments": [{"order_id": "O0", "status": "captured", "amount_cents": 277225}],
            "products": [{"product_id": "P1"}],
        }
        specs = {}
        for name, rows in tables.items():
            payload = "\n".join(json.dumps(row) for row in rows).encode()
            filename = name + ".jsonl"
            (fixture / filename).write_bytes(payload)
            specs[name] = {"file": filename, "sha256": hashlib.sha256(payload).hexdigest(),
                           "rows": len(rows)}
        config = {"batch_at": (start + timedelta(days=index)).isoformat(),
                  "source_semantics": "full_snapshot", "customers": 10000,
                  "orders": 100, "customer_revision": index}
        write(fixture / "manifest.json", {"batch_id": batch, "config": config, "tables": specs})
        checkpoint[batch] = {"manifest_sha256": hashlib.sha256(
            (fixture / "manifest.json").read_bytes()).hexdigest()}
        periods = [{"valid_from": (start + timedelta(days=j)).isoformat(),
                    "valid_to": (start + timedelta(days=j + 1)).isoformat() if j < index else None,
                    "is_current": j == index, "rows": 10000, "customers": 10000,
                    "matching_customers": 10000} for j in range(n)]
        query = {"rows": [{"orders": 100, "revenue": 277225}]}
        samples.append({"batch_id": batch, "prior_completed_batches": index,
            "run": {"status": "ready", "completed_stages": [
                "bronze-input-sync", "spark-commerce-bronze", "spark-commerce-payment-silver",
                "spark-commerce-order-silver", "spark-commerce-product-silver",
                "spark-commerce-customer-silver", "spark-commerce-customer-scd2",
                "spark-commerce-daily-gold", "spark-commerce-current-gold"],
                "durations_seconds": {"total": 1, "stages": {"spark-commerce-customer-scd2": 1}}},
            "current_query": query, "query_costs": {"detail": query, "daily": query},
            "query_rows": [{"revenue": 277225}], "customer_history": {"rows": periods,
                "total_rows": 10000 * n, "current_rows": 10000},
            "idle_replay": "verified", "cumulative_current_files": [],
            "idle_replay_observation": {"status": "verified", "completed_stages": [],
                "files": [], "snapshots": [{"table": table, "snapshot_ids": list(range(n))}
                    for table in ("gold.dim_customers_scd2", "gold.commerce_current_daily")]},
            "memory": [{"status": "observed"}] * 8,
            "stack_memory": {"status": "observed", "peak_observed_bytes": 1024},
            "warehouse_storage": copy.deepcopy(storage)})
        write(root / f"sample-{n}.warehouse.json", storage)
    write(root / "profile.json", {"status": "ready", "samples": samples})
    write(root / "checkpoint.json", {"processed_batches": checkpoint})
    write(base / "customer-history-ci/experiment.json", {"status": "ready", "source_sha": SOURCE,
        "profile_sha256": hashlib.sha256((root / "profile.json").read_bytes()).hexdigest(),
        "workload": {"customers": 10000, "orders": 100, "batches": 100,
                     "customer_revisions": list(range(100))},
        "steps": [{"status": "ready", "returncode": 0}],
        "network_cleanup": {"status": "absent"}})
    return base


def test_all_hundred_synthetic_samples_checked(retained):
    result = module().audit(retained, snapshots=100)
    assert result["source_sha"] == SOURCE
    assert result["run_id"] == 38969869281
    assert len(result["samples"]) == 100
    assert result["samples"][-1]["history_rows"] == 1000000


@pytest.mark.parametrize("fault", [
    "period", "names", "revenue", "stage", "idle", "partial", "total", "storage", "count",
])
def test_last_sample_corruption_refused(retained, fault):
    root = retained / "customer-history-ci"
    profile_path, experiment_path = root / "profile/profile.json", root / "experiment.json"
    original = {p: p.read_bytes() for p in (profile_path, experiment_path)}
    profile = json.loads(original[profile_path])
    last = profile["samples"][-1]
    if fault == "period":
        last["customer_history"]["rows"][-1]["valid_to"] = "2026-06-01T00:00:00Z"
    elif fault == "names":
        last["customer_history"]["rows"][-1]["matching_customers"] = 9999
    elif fault == "revenue":
        last["current_query"]["rows"][0]["revenue"] = 1
    elif fault == "stage":
        last["run"]["completed_stages"].pop()
    elif fault == "idle":
        last["idle_replay_observation"]["completed_stages"] = ["unexpected"]
    elif fault == "total":
        last["customer_history"]["total_rows"] -= 10000
    elif fault == "storage":
        last["warehouse_storage"]["total_payload_bytes"] += 1
    elif fault == "count":
        profile["samples"].pop()
    else:
        profile["status"] = "failed"
    try:
        write(profile_path, profile)
        experiment = json.loads(original[experiment_path])
        experiment["profile_sha256"] = hashlib.sha256(profile_path.read_bytes()).hexdigest()
        write(experiment_path, experiment)
        with pytest.raises(AssertionError):
            module().audit(retained, snapshots=100)
    finally:
        for path, payload in original.items():
            path.write_bytes(payload)


def test_hundred_artifact_cannot_be_audited_as_fifty(retained):
    with pytest.raises(AssertionError):
        module().audit(retained)


@pytest.mark.parametrize("count", [True, 4, 99, 101])
def test_unapproved_depth_rejected_before_read(tmp_path, count):
    with pytest.raises(ValueError):
        module().audit(tmp_path / "missing", snapshots=count)


@pytest.mark.parametrize("fault", ["source", "checkpoint", "active_intent"])
def test_untrusted_or_incomplete_experiment_refused(retained, fault):
    root = retained / "customer-history-ci"
    runner = retained / "customer-history-runner/source-sha.txt"
    checkpoint = root / "profile/checkpoint.json"
    experiment = root / "experiment.json"
    active = root / "profile/checkpoint.json.active.json"
    original = {p: p.read_bytes() for p in (runner, checkpoint, experiment)}
    try:
        if fault == "source":
            wrong = "a" * 40
            runner.write_text(wrong)
            report = json.loads(original[experiment])
            report["source_sha"] = wrong
            write(experiment, report)
        elif fault == "checkpoint":
            report = json.loads(original[checkpoint])
            report["processed_batches"].pop("synthetic-99")
            write(checkpoint, report)
        else:
            write(active, {"batch_id": "synthetic-99"})
        with pytest.raises(AssertionError):
            module().audit(retained, snapshots=100)
    finally:
        active.unlink(missing_ok=True)
        for path, payload in original.items():
            path.write_bytes(payload)
