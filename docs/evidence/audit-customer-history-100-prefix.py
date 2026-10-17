"""Audit only the recorded 90-snapshot prefix of failed Actions run 38969869281.

This is not an admission of partial experiments as successful complete runs.
Run without Python -O, because verification uses assertions.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path


def load(path):
    return json.loads(path.read_bytes())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def audit(base: Path):
    base = base.resolve()
    raw = base / "customer-history-ci"
    root = raw / "profile"
    files = [p for p in base.rglob("*") if p.is_file()]
    before = {p.relative_to(base).as_posix(): sha(p) for p in files}
    spec = importlib.util.spec_from_file_location(
        "auditor", Path(__file__).with_name("audit-customer-history.py")
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    e, p, c = (
        load(raw / "experiment.json"),
        load(root / "profile.json"),
        load(root / "checkpoint.json")["processed_batches"],
    )
    assert (
        e["source_sha"]
        == (base / "customer-history-runner/source-sha.txt").read_text().strip()
        == "6459ed9595b2a387c7ff60f36727484160efedaa"
    )
    assert (
        e["status"] == "failed" and e["error_type"] == "TimeoutExpired" and p["status"] == "running"
    )
    assert e["workload"] == {
        "customers": 10000,
        "orders": 100,
        "batches": 100,
        "customer_revisions": list(range(100)),
    }
    assert e["cleanup_error"] == "demo network has attached containers; network retained"
    assert any(
        s["name"] == "profile" and s["status"] == "failed" and s["timeout_seconds"] == 19500
        for s in e["steps"]
    )
    assert len(p["samples"]) == len(c) == 90 and set(c) == {s["batch_id"] for s in p["samples"]}
    start = datetime(2026, 2, 1, tzinfo=UTC)
    summaries = []
    for i, s in enumerate(p["samples"]):
        n = i + 1
        fixture = root / f"sample-{n}" / f"batch_id={s['batch_id']}"
        manifest = load(fixture / "manifest.json")
        assert sha(fixture / "manifest.json") == c[s["batch_id"]]["manifest_sha256"]
        assert manifest["batch_id"] == s["batch_id"] and m.timestamp(
            manifest["config"]["batch_at"]
        ) == start + timedelta(days=i)
        assert (
            manifest["config"]["source_semantics"] == "full_snapshot"
            and manifest["config"].get("customer_revision", 0) == i
        )
        assert manifest["config"]["customers"] == 10000 and manifest["config"]["orders"] == 100
        assert set(manifest["tables"]) == {"customers", "orders", "products", "payments"}
        tables = {}
        for name, t in manifest["tables"].items():
            path = fixture / t["file"]
            assert path.parent == fixture and sha(path) == t["sha256"]
            rows = [json.loads(line) for line in path.read_bytes().splitlines()]
            assert len(rows) == t["rows"]
            tables[name] = rows
        assert len(tables["customers"]) == 10000
        for k, r in enumerate(tables["customers"], 1):
            assert r["customer_id"] == f"C{k:08d}" and r["full_name"] == f"Customer {k:08d}" + (
                f" revision {i}" if i else ""
            )
        orders = {r["order_id"] for r in tables["orders"]}
        revenue = sum(
            r["amount_cents"]
            for r in tables["payments"]
            if r["order_id"] in orders
            and r["status"] == "captured"
            and type(r["amount_cents"]) is int
            and r["amount_cents"] > 0
        )
        assert len(orders) == 100 and revenue == 277225
        assert (
            s["prior_completed_batches"] == i
            and s["run"]["status"] == "ready"
            and s["run"]["completed_stages"] == m.STAGES
        )
        for q in (s["current_query"], s["query_costs"]["detail"], s["query_costs"]["daily"]):
            assert q["rows"] == [{"orders": 100, "revenue": 277225}]
        assert s["query_rows"] == [{"revenue": 277225}]
        h = s["customer_history"]
        assert (
            h["status"] == "verified"
            and h["total_rows"] == 10000 * n
            and h["current_rows"] == 10000
            and len(h["rows"]) == n
        )
        for j, r in enumerate(h["rows"]):
            assert m.timestamp(r["valid_from"]) == start + timedelta(days=j)
            assert (m.timestamp(r["valid_to"]) if r["valid_to"] else None) == (
                start + timedelta(days=j + 1) if j < i else None
            )
            assert r["is_current"] is (j == i) and all(
                r[k] == 10000 for k in ("rows", "customers", "matching_customers")
            )
        idle = s["idle_replay_observation"]
        assert (
            s["idle_replay"] == idle["status"] == "verified"
            and idle["completed_stages"] == []
            and idle["files"] == s["cumulative_current_files"]
        )
        for table in ("gold.dim_customers_scd2", "gold.commerce_current_daily"):
            ids = next(x["snapshot_ids"] for x in idle["snapshots"] if x["table"] == table)
            assert len(ids) == len(set(ids)) == n
        assert (
            len(s["memory"]) == 8
            and all(x["status"] == "observed" for x in s["memory"])
            and s["stack_memory"]["status"] == "observed"
        )
        storage = load(root / f"sample-{n}.warehouse.json")
        assert storage == s["warehouse_storage"]
        groups = m.storage_groups(storage)
        summaries.append(
            {
                "snapshot": n,
                "batch_id": s["batch_id"],
                "history_rows": h["total_rows"],
                "warehouse_bytes": storage["total_payload_bytes"],
                "groups": groups,
            }
        )
    active = load(root / "checkpoint.json.active.json")["batch"]
    fixture = root / "sample-91" / f"batch_id={active['batch_id']}"
    assert (
        active["batch_id"] not in c and sha(fixture / "manifest.json") == active["manifest_sha256"]
    )
    assert m.timestamp(active["batch_at"]) == start + timedelta(days=90)
    assert before == {p.relative_to(base).as_posix(): sha(p) for p in files}
    report = {
        "status": "failed_experiment_completed_prefix_verified",
        "source_sha": e["source_sha"],
        "run_id": 38969869281,
        "artifact_id": 12087127273,
        "completed_samples": len(summaries),
        "active_batch": active,
        "raw_files_unchanged": len(files),
        "raw_sha256": before,
        "last_completed_observation": summaries[-1],
        "samples": summaries,
        "cleanup_error": e["cleanup_error"],
        "limits": [
            "not successful 100-snapshot run",
            "last observation is not final catalog state",
            "active intent does not establish commit outcome",
            "payload is not physical disk or billing",
        ],
    }
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact_directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(audit(args.artifact_directory), indent=2))
