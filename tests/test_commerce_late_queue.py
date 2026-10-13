from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest
from test_commerce_batches import FakeS3Client

from lakehouse_ops import commerce_pipeline
from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES, run_commerce_batch
from lakehouse_ops.ingestion.commerce_batches import CommerceBatchPlanner

CURRENT = "spark-commerce-current-gold"


def add_snapshot(client, batch_id, at, *, full=True):
    client.add_manifest(batch_id, at)
    key = f"landing/source=commerce/batch_id={batch_id}/manifest.json"
    body, metadata = client.objects[key]
    manifest = json.loads(body)
    if full:
        manifest["config"]["source_semantics"] = "full_snapshot"
    body = json.dumps(manifest).encode()
    client.objects[key] = (body, {**metadata, "sha256": hashlib.sha256(body).hexdigest()})


@pytest.mark.parametrize("failed_stage", [None, COMMERCE_STAGES[-1], CURRENT])
def test_late_full_snapshot_retry_then_forward_and_idle(tmp_path: Path, failed_stage):
    client = FakeS3Client()
    add_snapshot(client, "a" * 16, "2026-02-01T00:00:00Z")
    add_snapshot(client, "b" * 16, "2026-02-03T00:00:00Z")
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=tmp_path / "state.json")
    planner.commit("a" * 16)
    planner.commit("b" * 16)
    before = (tmp_path / "state.json").read_bytes()
    add_snapshot(client, "c" * 16, "2026-02-02T00:00:00Z")
    add_snapshot(client, "d" * 16, "2026-02-04T00:00:00Z")
    stages = []
    fail = failed_stage

    def stage(service, selected):
        assert (tmp_path / "state.json").read_bytes() == before
        assert selected["customer_history_mode"] == "rebuild"
        stages.append(service)
        if service == fail:
            raise RuntimeError("injected failure after history correction")

    def query(sql):
        assert stages[-1] == CURRENT
        return [{"days": 1, "orders": 1, "captured_revenue_cents": 1000, "invalid_days": 0}]

    if fail:
        with pytest.raises(RuntimeError, match="injected failure"):
            run_commerce_batch(planner, query, run_stage=stage)
        assert (tmp_path / "state.json").read_bytes() == before
        assert planner.plan(max_batches=1)["batches"][0]["batch_id"] == "c" * 16
        fail = None
        stages.clear()
    late = run_commerce_batch(planner, query, run_stage=stage)
    assert late["completed_stages"] == [*COMMERCE_STAGES, CURRENT]
    assert late["checkpoint"]["batch_id"] == "c" * 16
    following = []

    def forward(service, selected):
        assert selected["batch_id"] == "d" * 16
        assert selected["customer_history_mode"] == "forward"
        following.append(service)

    assert run_commerce_batch(planner, query, run_stage=forward)["status"] == "ready"
    assert following == [*COMMERCE_STAGES, CURRENT]
    assert run_commerce_batch(planner, query, run_stage=lambda *_: pytest.fail("idle compute"))[
        "status"
    ] == "idle"


def test_undeclared_source_does_not_enable_rebuild_or_current_publication(tmp_path):
    client = FakeS3Client()
    add_snapshot(client, "a" * 16, "2026-02-03T00:00:00Z", full=False)
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=tmp_path / "state.json")
    planner.commit("a" * 16)
    add_snapshot(client, "b" * 16, "2026-02-02T00:00:00Z", full=False)
    stages = []

    def stage(service, selected):
        assert "customer_history_mode" not in selected
        stages.append(service)

    run_commerce_batch(planner, lambda _: [{"days": 1, "orders": 1,
                       "captured_revenue_cents": 1, "invalid_days": 0}], run_stage=stage)
    assert stages == list(COMMERCE_STAGES)


@pytest.mark.parametrize("mode", ["forward", "rebuild"])
def test_compose_uses_selected_history_mode_without_changing_parent(monkeypatch, mode):
    monkeypatch.setenv("COMMERCE_CUSTOMER_HISTORY_MODE", "inherited")
    seen = []

    def run(command, **kwargs):
        seen.append(kwargs["env"]["COMMERCE_CUSTOMER_HISTORY_MODE"])

    monkeypatch.setattr(commerce_pipeline.subprocess, "run", run)
    commerce_pipeline.run_compose_stage("spark-commerce-customer-scd2", {
        "batch_id": "a" * 16, "manifest_sha256": "b" * 64, "customer_history_mode": mode,
    }, bucket="lakehouse")
    assert seen == [mode]
    assert os.environ["COMMERCE_CUSTOMER_HISTORY_MODE"] == "inherited"
