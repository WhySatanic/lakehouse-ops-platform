"""Read retained real runner evidence; never starts or mutates a stack."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from lakehouse_ops.commerce_journal_inspection import inspect_journal
from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES, CURRENT_STAGE


def check_retained_refund(root: Path) -> None:
    paths = [root / n for n in ("linked.json", "checkpoint.json", "run-journal.sqlite3")]
    before = [hashlib.sha256(p.read_bytes()).hexdigest() for p in paths]
    report = json.loads(paths[0].read_bytes())
    expected = json.loads((root / "scenario/expected.json").read_bytes())
    batches = expected["batches"]
    samples = report["samples"]
    assert report["status"] == "ready" and len(samples) == len(batches) == 5
    assert report["scope"] == "five_full_snapshots_refund_reference_stack"
    assert [s["oracle"]["captured_revenue_cents"] for s in samples] == [1000, 1500, 0, 700, 1000]
    checkpoint = json.loads(paths[1].read_bytes())["processed_batches"]
    assert set(checkpoint) == {b["batch_id"] for b in batches}
    assert not (root / "checkpoint.json.active.json").exists()
    for index, (sample, batch) in enumerate(zip(samples, batches, strict=True)):
        assert sample["status"] == sample["pipeline"]["status"] == "ready"
        assert sample["batch_id"] == sample["oracle"]["batch_id"] == batch["batch_id"]
        assert sample["pipeline"]["completed_stages"] == [*COMMERCE_STAGES, CURRENT_STAGE]
        manifest = root / "scenario" / f"batch_id={batch['batch_id']}" / "manifest.json"
        assert checkpoint[batch["batch_id"]]["manifest_sha256"] == (
            hashlib.sha256(manifest.read_bytes()).hexdigest()
        )
        historical = sample["oracle"]["historical_daily"]
        assert {row["source_batch_id"] for row in historical} == {
            b["batch_id"] for b in batches[:index + 1]
        }
        assert [sum(row["captured_revenue_cents"] for row in historical
                    if row["source_batch_id"] == b["batch_id"])
                for b in batches[:index + 1]] == [1000, 1500, 0, 700, 1000][:index + 1]
    assert report["refund_replay"]["status"] == "ready"
    assert report["refund_replay"]["oracle"]["captured_revenue_cents"] == 0
    assert report["refund_replay"]["snapshots"] == [{"snapshots": 3}]
    assert report["current_mart_replay"] == "snapshot_stable"
    assert report["idle"]["status"] == "idle" and report["idle"]["completed_stages"] == []
    runs = inspect_journal(paths[2])["runs"]
    assert len(runs) == 7 and all(r["outcome"] == "ready" for r in runs)
    assert sorted(r["checkpoint_completed_events"] for r in runs) == [0, 0, 1, 1, 1, 1, 1]
    assert [hashlib.sha256(p.read_bytes()).hexdigest() for p in paths] == before


@pytest.mark.skipif(not os.environ.get("LAKEOPS_REFUND_DIRECTORY"),
                    reason="requires retained real five-snapshot refund runner evidence")
def test_retained_refund_runner_and_checkpoint():
    check_retained_refund(Path(os.environ["LAKEOPS_REFUND_DIRECTORY"]).resolve())
