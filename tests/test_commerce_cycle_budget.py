from __future__ import annotations

import subprocess
import sys
import time

import pytest
from test_commerce_batches import FakeS3Client

from lakehouse_ops import commerce_cycle as cycle
from lakehouse_ops import commerce_pipeline as pipeline
from lakehouse_ops.ingestion.commerce_batches import CommerceBatchPlanner

OPTIONS = [
    "--s3-bucket", "lakehouse", "--s3-endpoint-url", "http://localhost:9000",
    "--state", "unused.json", "--server", "http://localhost:8080", "--user", "lakehouse-ops",
    "--instance", "commerce-local", "--alertmanager-server", "http://localhost:9093",
]


@pytest.mark.parametrize("boundary", ["preflight", "spawn", "subsecond"])
def test_cycle_deadline_reaches_checkpoint_without_child_budget_reset(
    tmp_path, monkeypatch, boundary,
):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "30")
    now = [0.0]
    client = FakeS3Client()
    client.add_manifest("a" * 16, "2026-02-01T00:00:00Z")
    state = tmp_path / "checkpoint.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)
    child_outcomes = []
    validate = cycle.validate_commerce_gold_retry

    def preflight(*args):
        validate(*args)
        now[0] = {"preflight": 0.25, "spawn": 0, "subsecond": 29.25}[boundary]

    def query(_):
        now[0] = 30.1
        return [{"days": 1, "orders": 1, "captured_revenue_cents": 100, "invalid_days": 0}]

    def child(command, **kwargs):
        assert command[3] == "run-commerce-batch", "expired cycle must skip notification"
        if boundary == "spawn":
            now[0] = 1.25
        with monkeypatch.context() as child_environment:
            for key, value in kwargs["env"].items():
                child_environment.setenv(key, value)
            try:
                result = pipeline.run_commerce_batch(
                    planner, query, run_stage=lambda *_: None, clock=lambda: 1000 + now[0],
                )
            except pipeline.CommercePipelineError:
                child_outcomes.append("expired")
                return subprocess.CompletedProcess(command, 2)
            child_outcomes.append(result["status"])
            return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cycle.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(cycle, "validate_commerce_gold_retry", preflight)
    monkeypatch.setattr(cycle.subprocess, "run", child)
    assert cycle.main(OPTIONS) == 2
    assert not state.exists(), "child checkpoint exceeded the scheduled cycle's deadline"
    assert child_outcomes == ["expired"]
    # A standalone retry gets its ordinary fresh budget, completes and then idles.
    now[0] = 0.0
    assert pipeline.run_commerce_batch(
        planner, lambda _: [{"days": 1, "orders": 1, "captured_revenue_cents": 100,
                             "invalid_days": 0}],
        run_stage=lambda *_: None, clock=lambda: now[0],
    )["status"] == "ready"
    assert pipeline.run_commerce_batch(
        planner, query, run_stage=lambda *_: pytest.fail("idle must not compute"),
    )["status"] == "idle"


@pytest.mark.parametrize("elapsed", [4, 10])
def test_notification_uses_only_unspent_cycle_budget(monkeypatch, elapsed, capsys):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "10")
    now = 0
    calls = []

    def run(command, **kwargs):
        nonlocal now
        calls.append(kwargs)
        if len(calls) == 1:
            now = elapsed
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cycle.time, "monotonic", lambda: now)
    monkeypatch.setattr(cycle.subprocess, "run", run)
    assert cycle.main(OPTIONS) == (0 if elapsed < 10 else 2)
    assert calls[0]["timeout"] == 45
    assert calls[0]["env"]["COMMERCE_RUN_TIMEOUT_SECONDS"] == "10"
    if elapsed < 10:
        assert calls[1]["timeout"] == 6
    else:
        assert len(calls) == 1
        assert "notification skipped" in capsys.readouterr().err


@pytest.mark.parametrize("value", ["0", "86401", "bad"])
def test_invalid_cycle_budget_starts_no_children(monkeypatch, value):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", value)
    monkeypatch.setattr(cycle.subprocess, "run", lambda *a, **kw: pytest.fail("no child"))
    with pytest.raises(SystemExit) as error:
        cycle.main(OPTIONS)
    assert error.value.code == 2


def test_timeout_is_failure_and_does_not_claim_container_cleanup(monkeypatch, capsys):
    def run(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    monkeypatch.setattr(cycle.subprocess, "run", run)
    assert cycle._run_command(["unused"], "run-commerce-batch", timeout=1) == 2
    assert "container cleanup unconfirmed" in capsys.readouterr().err


def test_real_sleeping_child_is_bounded(capsys):
    started = time.monotonic()
    code = cycle._run_command(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        "notify-commerce-freshness", timeout=0.2,
    )
    assert code == 2
    assert time.monotonic() - started < 5
    assert "timed out" in capsys.readouterr().err


@pytest.mark.parametrize("value", ["", "nan", "inf", "-inf", "bad"])
def test_invalid_inherited_deadline_starts_no_discovery(monkeypatch, value):
    monkeypatch.setenv(pipeline.COMMERCE_CYCLE_DEADLINE_ENV, value)
    planner = object()
    with pytest.raises(pipeline.CommercePipelineError, match="invalid inherited"):
        pipeline.run_commerce_batch(planner, lambda _: [], run_stage=lambda *_: None)


def test_deadline_expired_during_child_startup_starts_no_discovery(monkeypatch):
    monkeypatch.setenv(pipeline.COMMERCE_CYCLE_DEADLINE_ENV, "10")
    monkeypatch.setattr(pipeline.time, "monotonic", lambda: 11)
    with pytest.raises(pipeline.CommercePipelineError, match="time budget exhausted"):
        pipeline.run_commerce_batch(object(), lambda _: [], run_stage=lambda *_: None)


def test_parent_deadline_cannot_extend_shorter_standalone_budget(monkeypatch):
    from test_commerce_pipeline import Planner, gold

    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv(pipeline.COMMERCE_CYCLE_DEADLINE_ENV, "1000")
    monkeypatch.setattr(pipeline.time, "monotonic", lambda: 0)
    now = [0]
    planner = Planner()

    def stage(*_):
        now[0] = 2

    with pytest.raises(pipeline.CommercePipelineError, match="time budget exhausted"):
        pipeline.run_commerce_batch(planner, gold, run_stage=stage, clock=lambda: now[0])
    assert planner.commits == []


def test_checkpoint_rechecks_original_parent_clock_after_fsync(tmp_path, monkeypatch):
    from lakehouse_ops.ingestion import commerce_batches

    monkeypatch.setenv(pipeline.COMMERCE_CYCLE_DEADLINE_ENV, "1")
    now = [0]
    monkeypatch.setattr(pipeline.time, "monotonic", lambda: now[0])
    client = FakeS3Client()
    client.add_manifest("a" * 16, "2026-02-01T00:00:00Z")
    state = tmp_path / "checkpoint.json"
    planner = CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)
    fsync = commerce_batches.os.fsync
    armed = [False]
    commit = planner.commit

    def checkpoint(*args, **kwargs):
        armed[0] = True
        return commit(*args, **kwargs)

    def sync(descriptor):
        fsync(descriptor)
        if armed[0]:
            now[0] = 2

    monkeypatch.setattr(planner, "commit", checkpoint)
    monkeypatch.setattr(commerce_batches.os, "fsync", sync)
    with pytest.raises(pipeline.CommercePipelineError, match="time budget exhausted"):
        pipeline.run_commerce_batch(
            planner, lambda _: [{"days": 1, "orders": 1, "captured_revenue_cents": 100,
                                 "invalid_days": 0}],
            run_stage=lambda *_: None, clock=lambda: 1000,
        )
    assert not state.exists()
    assert state.with_name("checkpoint.json.active.json").is_file()
    assert not list(tmp_path.glob(".checkpoint.json.*"))
