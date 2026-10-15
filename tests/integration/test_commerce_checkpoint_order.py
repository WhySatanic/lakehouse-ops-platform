from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import boto3
import pytest

from lakehouse_ops import commerce_cycle
from lakehouse_ops.commerce_change_scenario import generate_change_scenario
from lakehouse_ops.commerce_pipeline import (
    COMMERCE_STAGES,
    CURRENT_STAGE,
    CommercePipelineError,
    run_commerce_batch,
)
from lakehouse_ops.ingestion.commerce_batches import CommerceBatchError, CommerceBatchPlanner
from lakehouse_ops.ingestion.commerce_fixture import (
    CommerceFixtureConfig,
    generate_commerce_fixture,
)
from lakehouse_ops.ingestion.commerce_s3_landing import CommerceS3LandingZone


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_S3_SMOKE") != "1", reason="requires an initialized local MinIO"
)
def test_real_minio_planning_timing_includes_precompute_discovery(tmp_path):
    client = boto3.client(
        "s3", endpoint_url=os.getenv("LAKEOPS_S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "lakeops"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "lakeops-development-only"),
        region_name="us-east-1",
    )
    bucket = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
    prefix = f"smoke/commerce-phase-timing-{uuid4().hex}"
    scenario = generate_change_scenario(tmp_path / "scenario")
    first = scenario["batches"][0]
    CommerceS3LandingZone(client, bucket=bucket, prefix=prefix).write(Path(first["path"]))
    now = [0.0]
    discoveries = []

    class TimedPlanner(CommerceBatchPlanner):
        def discover(self):
            batches = super().discover()
            discoveries.append(len(batches))
            now[0] += 1
            return batches

    state = tmp_path / "checkpoint.json"
    planner = TimedPlanner(client, bucket=bucket, prefix=prefix, state_path=state)
    stages = []

    def stage(service, batch):
        assert batch["customer_history_mode"] == "forward"
        assert not state.exists()
        assert state.with_name("checkpoint.json.active.json").is_file()
        stages.append(service)
        now[0] += 2

    def query(_):
        assert stages == [*COMMERCE_STAGES, CURRENT_STAGE] and not state.exists()
        now[0] += 3
        return [{"days": 1, "orders": 1, "captured_revenue_cents": 1000, "invalid_days": 0}]

    result = run_commerce_batch(planner, query, run_stage=stage, clock=lambda: now[0])
    assert discoveries == [1, 1, 1, 1]
    assert result["durations_seconds"] == {
        "planning": 3, "stages": dict.fromkeys([*COMMERCE_STAGES, CURRENT_STAGE], 2),
        "verification": 3, "checkpoint": 1, "total": 25,
    }
    checkpoint = state.read_bytes()
    assert set(json.loads(checkpoint)["processed_batches"]) == {first["batch_id"]}
    assert not state.with_name("checkpoint.json.active.json").exists()

    def forbidden(*_):
        pytest.fail("idle must not compute or verify gold")

    assert run_commerce_batch(planner, forbidden, run_stage=forbidden) == {
        "status": "idle", "batch_id": None, "completed_stages": [],
    }
    assert state.read_bytes() == checkpoint


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_S3_SMOKE") != "1", reason="requires an initialized local MinIO"
)
@pytest.mark.parametrize("delay_phase", ["startup", "query"])
def test_real_child_cycle_deadline_preserves_checkpoint_and_retries(
    tmp_path, monkeypatch, delay_phase,
):
    endpoint = os.getenv("LAKEOPS_S3_ENDPOINT_URL", "http://localhost:9000")
    client = boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "lakeops"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "lakeops-development-only"),
        region_name="us-east-1",
    )
    bucket = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
    prefix = f"smoke/commerce-cycle-deadline-{uuid4().hex}"
    fixture = generate_commerce_fixture(tmp_path / "fixture", CommerceFixtureConfig(
        customers=4, products=2, orders=6, null_customer_emails=1, duplicate_orders=1,
        late_orders=1, invalid_payments=1, batch_at="2026-02-01T00:00:00Z",
    ))
    CommerceS3LandingZone(client, bucket=bucket, prefix=prefix).write(fixture.path)
    state = tmp_path / "checkpoint.json"
    script = '''
import json, os, sys, time
import boto3
from lakehouse_ops.commerce_pipeline import CommercePipelineError, run_commerce_batch
from lakehouse_ops.ingestion.commerce_batches import CommerceBatchPlanner
from pathlib import Path
state, prefix, bucket, endpoint, phase = sys.argv[1:]
if phase == "startup":
    time.sleep(5.1)
client = boto3.client("s3", endpoint_url=endpoint,
    aws_access_key_id=os.getenv("MINIO_ROOT_USER", "lakeops"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "lakeops-development-only"),
    region_name="us-east-1")
planner = CommerceBatchPlanner(client, bucket=bucket, prefix=prefix, state_path=Path(state))
def query(_):
    if phase == "query":
        time.sleep(4.5)
    return [{"days": 1, "orders": 1, "captured_revenue_cents": 100, "invalid_days": 0}]
try:
    report = run_commerce_batch(planner, query, run_stage=lambda *_: None)
except CommercePipelineError as error:
    print(str(error), file=sys.stderr)
    raise SystemExit(2)
print(json.dumps({"status": report["status"]}))
'''
    run = subprocess.run
    phase = [delay_phase]
    results = []

    def launch(command, **kwargs):
        if command[3] == "notify-commerce-freshness":
            return subprocess.CompletedProcess(command, 0)
        if phase[0] == "query":
            time.sleep(0.6)
        result = run(
            [sys.executable, "-c", script, str(state), prefix, bucket, endpoint, phase[0]],
            **kwargs, capture_output=True, text=True,
        )
        results.append(result)
        return result

    monkeypatch.setattr(commerce_cycle.subprocess, "run", launch)
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "5")
    options = [
        "--s3-bucket", bucket, "--s3-endpoint-url", endpoint, "--state", str(state),
        "--server", "http://localhost:8080", "--user", "lakehouse-ops",
        "--instance", "commerce-smoke", "--alertmanager-server", "http://localhost:9093",
    ]
    assert commerce_cycle.main(options) == 2
    assert "time budget exhausted" in results[0].stderr
    assert not state.exists()
    if delay_phase == "query":
        assert state.with_name("checkpoint.json.active.json").is_file()
    phase[0] = "retry"
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "10")
    assert commerce_cycle.main(options) == 0
    assert json.loads(results[-1].stdout)["status"] == "ready"
    checkpoint = state.read_bytes()
    assert set(json.loads(checkpoint)["processed_batches"]) == {fixture.batch_id}
    assert not state.with_name("checkpoint.json.active.json").exists()
    assert commerce_cycle.main(options) == 0
    assert json.loads(results[-1].stdout)["status"] == "idle"
    assert state.read_bytes() == checkpoint


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_S3_SMOKE") != "1", reason="requires an initialized local MinIO"
)
def test_real_minio_checkpoint_cannot_skip_older_batch(tmp_path: Path) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("LAKEOPS_S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "lakeops"),
        aws_secret_access_key=os.getenv(
            "MINIO_ROOT_PASSWORD", "lakeops-development-only"
        ),
        region_name="us-east-1",
    )
    bucket = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
    prefix = f"smoke/commerce-checkpoint-{uuid4().hex}"
    landing = CommerceS3LandingZone(client, bucket=bucket, prefix=prefix)
    config = {
        "customers": 4,
        "products": 2,
        "orders": 6,
        "null_customer_emails": 1,
        "duplicate_orders": 1,
        "late_orders": 1,
        "invalid_payments": 1,
    }
    later = generate_commerce_fixture(
        tmp_path / "fixture", CommerceFixtureConfig(**config, batch_at="2026-02-01T00:00:00+00:00")
    )
    earlier = generate_commerce_fixture(
        tmp_path / "fixture", CommerceFixtureConfig(**config, batch_at="2026-01-01T00:00:00+00:00")
    )
    landing.write(later.path)
    landing.write(earlier.path)
    state_path = tmp_path / "checkpoint.json"
    planner = CommerceBatchPlanner(
        client, bucket=bucket, prefix=prefix, state_path=state_path
    )
    assert planner.check_backlog_freshness(max_age_seconds=900)["pending_batches"] == 2
    overdue = planner.check_backlog_freshness(
        max_age_seconds=900, now=datetime.now(UTC) + timedelta(hours=1)
    )
    assert overdue["status"] == "stale"
    assert overdue["pending_batches"] == 2

    with pytest.raises(CommerceBatchError, match="earlier unprocessed batch"):
        planner.commit(later.batch_id)

    assert not state_path.exists()
    assert planner.commit(earlier.batch_id)["created"] is True
    assert planner.commit(later.batch_id)["created"] is True
    assert planner.plan(max_batches=1)["selected_batches"] == 0
    assert planner.check_backlog_freshness(max_age_seconds=900)["pending_batches"] == 0


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_S3_SMOKE") != "1", reason="requires an initialized local MinIO"
)
def test_real_minio_related_snapshots_land_replay_and_plan_in_order(tmp_path: Path) -> None:
    client = boto3.client(
        "s3", endpoint_url=os.getenv("LAKEOPS_S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "lakeops"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "lakeops-development-only"),
        region_name="us-east-1",
    )
    bucket = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
    prefix = f"smoke/commerce-changes-{uuid4().hex}"
    scenario = generate_change_scenario(tmp_path / "scenario")
    landing = CommerceS3LandingZone(client, bucket=bucket, prefix=prefix)
    # Publication order differs from event-processing order.
    for batch in reversed(scenario["batches"]):
        assert landing.write(Path(batch["path"])).created == 5
        assert landing.write(Path(batch["path"])).created == 0
    planner = CommerceBatchPlanner(
        client, bucket=bucket, prefix=prefix, state_path=tmp_path / "checkpoint.json"
    )
    for batch in scenario["batches"]:
        plan = planner.plan(max_batches=1)
        selected = plan["batches"][0]
        assert selected["batch_id"] == batch["batch_id"]
        assert selected["batch_at"] == batch["batch_at"]
        planner.commit(batch["batch_id"], expected_manifest_sha256=selected["manifest_sha256"])
    assert planner.plan(max_batches=1)["selected_batches"] == 0
    replay = planner.plan(max_batches=1, replay_batches=(scenario["batches"][1]["batch_id"],))
    assert replay["mode"] == "replay"
    assert replay["batches"][0]["batch_id"] == scenario["batches"][1]["batch_id"]


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_S3_SMOKE") != "1", reason="requires an initialized local MinIO"
)
def test_real_minio_expired_checkpoint_read_preserves_state_and_retries(tmp_path, monkeypatch):
    client = boto3.client(
        "s3", endpoint_url=os.getenv("LAKEOPS_S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "lakeops"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "lakeops-development-only"),
        region_name="us-east-1",
    )
    bucket = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
    prefix = f"smoke/commerce-budget-{uuid4().hex}"
    scenario = generate_change_scenario(tmp_path / "scenario")
    landing = CommerceS3LandingZone(client, bucket=bucket, prefix=prefix)
    for batch in scenario["batches"][:2]:
        landing.write(Path(batch["path"]))
    state = tmp_path / "checkpoint.json"
    planner = CommerceBatchPlanner(client, bucket=bucket, prefix=prefix, state_path=state)
    planner.commit(scenario["batches"][0]["batch_id"])
    before = state.read_bytes()
    now = [0.0]
    armed = False
    list_objects = client.list_objects_v2

    def listing(**kwargs):
        result = list_objects(**kwargs)
        if armed:
            now[0] = 2.0
        return result

    def query(_):
        nonlocal armed
        armed = True
        return [{"days": 1, "orders": 1, "captured_revenue_cents": 1500, "invalid_days": 0}]

    monkeypatch.setattr(client, "list_objects_v2", listing)
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "1")
    with pytest.raises(CommercePipelineError, match="run time budget exhausted"):
        run_commerce_batch(planner, query, run_stage=lambda *_: None, clock=lambda: now[0])
    assert state.read_bytes() == before
    armed = False
    now[0] = 0.0
    assert run_commerce_batch(
        planner, lambda _: [{"days": 1, "orders": 1, "captured_revenue_cents": 1500,
                             "invalid_days": 0}],
        run_stage=lambda *_: None, clock=lambda: now[0],
    )["status"] == "ready"
    assert planner.plan(max_batches=1)["batches"] == []
