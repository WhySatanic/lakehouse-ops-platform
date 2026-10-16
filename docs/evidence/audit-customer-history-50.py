"""Audit a downloaded run 38922050656 artifact without Docker or catalog access.

Usage: python docs/evidence/audit-customer-history-50.py ARTIFACT_DIRECTORY
Prints a compact report; never modifies the retained experiment files.
"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

SOURCE = "d53976d2378ed578e7fb19df18de10c8436070a7"
STAGES = [
    "bronze-input-sync", "spark-commerce-bronze", "spark-commerce-payment-silver",
    "spark-commerce-order-silver", "spark-commerce-product-silver",
    "spark-commerce-customer-silver", "spark-commerce-customer-scd2",
    "spark-commerce-daily-gold", "spark-commerce-current-gold",
]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path):
    return json.loads(path.read_bytes())


def timestamp(value):
    return datetime.fromisoformat(value.replace(" UTC", "+00:00").replace("Z", "+00:00"))


def storage_groups(storage):
    """Recompute disjoint object groups from raw references and S3 versions."""
    current, retained = {}, {}
    accounting = storage["reference_accounting"]
    for table in accounting["references"]:
        for label, target in (("current", current), ("retained", retained)):
            for ref in table[label]:
                uri = urlsplit(ref["file_path"])
                assert uri.scheme in ("s3", "s3a") and uri.netloc == storage["bucket"]
                key = uri.path.lstrip("/")
                entry = (ref["bytes"], ref["content"])
                assert key not in target or target[key] == entry
                target[key] = entry
    groups = {name: {"versions": 0, "bytes": 0} for name in accounting["groups"]}
    found = set()
    identities = [(obj["key"], obj["version_id"]) for obj in storage["objects"]]
    assert len(identities) == len(set(identities))
    for obj in storage["objects"]:
        key = obj["key"]
        if not obj["is_latest"]:
            group = "s3_noncurrent"
        elif key in current or key in retained:
            size, content = (current if key in current else retained)[key]
            assert size == obj["bytes"] and content in (0, 1, 2)
            group = ("current" if key in current else "retained_only") + (
                "_data" if content == 0 else "_delete"
            )
            found.add(key)
        else:
            group = "latest_not_in_selected_file_references"
        groups[group]["versions"] += 1
        groups[group]["bytes"] += obj["bytes"]
    assert found == current.keys() | retained.keys()
    assert groups == accounting["groups"]
    total = sum(obj["bytes"] for obj in storage["objects"])
    assert total == storage["total_payload_bytes"] == accounting["total_payload_bytes"]
    assert total == sum(group["bytes"] for group in groups.values())
    assert accounting["status"] == "reconciled"
    assert accounting["orphan_classification"] == "not_performed"
    return groups


def audit(base: Path):
    raw = base / "customer-history-ci"
    root = raw / "profile"
    files = [p for p in base.rglob("*") if p.is_file()]
    before = {str(p.relative_to(base)): sha(p) for p in files}
    experiment, profile = load(raw / "experiment.json"), load(root / "profile.json")
    assert (base / "customer-history-runner/source-sha.txt").read_text().strip() == SOURCE
    assert experiment["source_sha"] == SOURCE and experiment["status"] == "ready"
    assert experiment["profile_sha256"] == sha(root / "profile.json")
    assert experiment["workload"] == {
        "customers": 10000, "orders": 100, "batches": 50,
        "customer_revisions": list(range(50)),
    }
    assert all(s["status"] == "ready" and s["returncode"] == 0
               for s in experiment["steps"])
    assert experiment["network_cleanup"]["status"] == "absent"
    assert profile["status"] == "ready" and len(profile["samples"]) == 50
    checkpoint = load(root / "checkpoint.json")["processed_batches"]
    assert len(checkpoint) == 50 and not (root / "checkpoint.json.active.json").exists()
    assert set(checkpoint) == {s["batch_id"] for s in profile["samples"]}
    start = datetime(2026, 2, 1, tzinfo=UTC)
    summaries = []
    for index, sample in enumerate(profile["samples"]):
        n, batch = index + 1, sample["batch_id"]
        assert sample["prior_completed_batches"] == index
        fixture = root / f"sample-{n}" / f"batch_id={batch}"
        manifest = load(fixture / "manifest.json")
        assert sha(fixture / "manifest.json") == checkpoint[batch]["manifest_sha256"]
        assert timestamp(manifest["config"]["batch_at"]) == start + timedelta(days=index)
        assert manifest["config"]["source_semantics"] == "full_snapshot"
        tables = {}
        for name, table in manifest["tables"].items():
            path = fixture / table["file"]
            assert path.parent == fixture and sha(path) == table["sha256"]
            rows = [json.loads(line) for line in path.read_bytes().splitlines()]
            assert len(rows) == table["rows"]
            tables[name] = rows
        customers = tables["customers"]
        assert len(customers) == len({c["customer_id"] for c in customers}) == 10000
        for customer_n, customer in enumerate(customers, 1):
            assert customer["customer_id"] == f"C{customer_n:08d}"
            suffix = f" revision {index}" if index else ""
            assert customer["full_name"] == f"Customer {customer_n:08d}{suffix}"
        orders = {row["order_id"] for row in tables["orders"]}
        revenue = sum(row["amount_cents"] for row in tables["payments"]
                      if row["order_id"] in orders and row["status"] == "captured"
                      and type(row["amount_cents"]) is int and row["amount_cents"] > 0)
        assert len(orders) == 100 and revenue == 277225
        assert sample["run"]["status"] == "ready"
        assert sample["run"]["completed_stages"] == STAGES
        for query in (sample["current_query"], sample["query_costs"]["detail"],
                      sample["query_costs"]["daily"]):
            assert query["rows"] == [{"orders": 100, "revenue": 277225}]
        assert sample["query_rows"] == [{"revenue": 277225}]
        history = sample["customer_history"]
        assert len(history["rows"]) == n and history["total_rows"] == 10000 * n
        assert history["current_rows"] == 10000
        for period, row in enumerate(history["rows"]):
            assert timestamp(row["valid_from"]) == start + timedelta(days=period)
            assert (timestamp(row["valid_to"]) if row["valid_to"] else None) == (
                start + timedelta(days=period + 1) if period < index else None
            )
            assert row["is_current"] is (period == index)
            assert all(row[k] == 10000 for k in ("rows", "customers", "matching_customers"))
        idle = sample["idle_replay_observation"]
        assert sample["idle_replay"] == idle["status"] == "verified"
        assert idle["completed_stages"] == []
        assert idle["files"] == sample["cumulative_current_files"]
        for table in ("gold.dim_customers_scd2", "gold.commerce_current_daily"):
            snapshots = next(x["snapshot_ids"] for x in idle["snapshots"]
                             if x["table"] == table)
            assert len(snapshots) == len(set(snapshots)) == n
        assert len(sample["memory"]) == 8
        assert all(m["status"] == "observed" for m in sample["memory"])
        assert sample["stack_memory"]["status"] == "observed"
        storage = load(root / f"sample-{n}.warehouse.json")
        assert storage == sample["warehouse_storage"]
        groups = storage_groups(storage)
        summaries.append({
            "snapshot": n, "batch_id": batch, "history_rows": n * 10000,
            "manifest_sha256": checkpoint[batch]["manifest_sha256"],
            "runner_seconds": sample["run"]["durations_seconds"]["total"],
            "scd2_seconds": sample["run"]["durations_seconds"]["stages"][STAGES[6]],
            "stack_sampled_peak_bytes": sample["stack_memory"]["peak_observed_bytes"],
            "warehouse_payload_bytes": storage["total_payload_bytes"], "groups": groups,
        })
    assert before == {str(p.relative_to(base)): sha(p) for p in files}
    return {"status": "verified", "source_sha": SOURCE, "run_id": 38922050656,
            "artifact_files_unchanged": len(files), "raw_sha256": {
                "experiment.json": sha(raw / "experiment.json"),
                "profile.json": sha(root / "profile.json"),
                "checkpoint.json": sha(root / "checkpoint.json"),
            }, "samples": summaries}


if __name__ == "__main__":
    print(json.dumps(audit(Path(sys.argv[1]).resolve()), indent=2))
