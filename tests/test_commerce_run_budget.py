from __future__ import annotations

import io
import subprocess
import time
from threading import Event

import pytest
from test_commerce_batches import FakeS3Client
from test_commerce_pipeline import BATCH, Planner, gold

from lakehouse_ops import commerce_pipeline as pipeline
from lakehouse_ops.ingestion import commerce_batches


@pytest.mark.parametrize("phase", ["listing", "manifest", "stream", "replacement"])
def test_checkpoint_budget_expires_without_write_and_retry_succeeds(tmp_path, monkeypatch, phase):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "1")
    now = [0.0]
    armed = False

    class Client(FakeS3Client):
        def list_objects_v2(self, **kwargs):
            if armed and phase == "listing":
                now[0] = 2.0
            return super().list_objects_v2(**kwargs)

        def get_object(self, **kwargs):
            response = super().get_object(**kwargs)
            if armed and phase == "manifest":
                now[0] = 2.0
            if armed and phase == "stream":
                read = response["Body"].read

                def slow_read(*args):
                    now[0] = 2.0
                    return read(*args)

                response["Body"].read = slow_read
            return response

    client = Client()
    client.add_manifest("a" * 16, "2026-02-01T00:00:00Z")
    client.add_manifest("b" * 16, "2026-02-02T00:00:00Z")
    state = tmp_path / "checkpoint.json"
    planner = commerce_batches.CommerceBatchPlanner(client, bucket="lakehouse", state_path=state)
    planner.commit("a" * 16)
    before = state.read_bytes()
    commit = planner.commit
    fsync = commerce_batches.os.fsync

    def delayed_fsync(descriptor):
        if armed and phase == "replacement":
            now[0] = 2.0
        return fsync(descriptor)

    def checkpoint(*args, **kwargs):
        nonlocal armed
        armed = True
        return commit(*args, **kwargs)

    monkeypatch.setattr(commerce_batches.os, "fsync", delayed_fsync)
    monkeypatch.setattr(planner, "commit", checkpoint)
    def query(_):
        return [{"days": 1, "orders": 1, "captured_revenue_cents": 100, "invalid_days": 0}]
    with pytest.raises(pipeline.CommercePipelineError, match="run time budget exhausted"):
        pipeline.run_commerce_batch(planner, query, run_stage=lambda *_: None, clock=lambda: now[0])
    assert state.read_bytes() == before
    assert list(tmp_path.glob(".checkpoint.json.*")) == []
    monkeypatch.setattr(planner, "commit", commit)
    armed = False
    now[0] = 0.0
    assert pipeline.run_commerce_batch(planner, query, run_stage=lambda *_: None,
                                       clock=lambda: now[0])["status"] == "ready"
    assert pipeline.run_commerce_batch(
        planner, query, run_stage=lambda *_: None,
    )["status"] == "idle"


@pytest.mark.parametrize("phase", ["listing", "manifest", "stream"])
@pytest.mark.parametrize("workers", [1, 8])
def test_blocked_s3_read_returns_at_deadline_and_closes_stream(tmp_path, phase, workers):
    release = Event()
    closed = Event()

    class Client(FakeS3Client):
        def list_objects_v2(self, **kwargs):
            if phase == "listing":
                release.wait(3)
            return super().list_objects_v2(**kwargs)

        def get_object(self, **kwargs):
            response = super().get_object(**kwargs)
            body = response["Body"]
            close = body.close
            read = body.read

            def close_body():
                close()
                closed.set()

            def read_body(*args):
                release.wait(3)
                return read(*args)

            body.close = close_body
            if phase == "stream":
                body.read = read_body
            if phase == "manifest":
                release.wait(3)
            return response

    client = Client()
    client.add_manifest("a" * 16, "2026-02-01T00:00:00Z")
    client.add_manifest("b" * 16, "2026-02-02T00:00:00Z")
    planner = commerce_batches.CommerceBatchPlanner(
        client, bucket="lakehouse", state_path=tmp_path / "state.json",
        manifest_read_workers=workers,
    )
    started = time.perf_counter()
    deadline = started + 0.1

    def check():
        remaining = deadline - time.perf_counter()
        if remaining <= 0:
            raise pipeline.CommercePipelineError("run time budget exhausted")
        return remaining

    try:
        with planner.execution_budget(check), pytest.raises(pipeline.CommercePipelineError):
            planner.plan(max_batches=1)
        assert time.perf_counter() - started < 1.0
        assert not (tmp_path / "state.json").exists()
    finally:
        release.set()
    if phase != "listing":
        assert closed.wait(1), "abandoned manifest response leaked its body"
    assert planner.plan(max_batches=1)["batches"][0]["batch_id"] == "a" * 16


def test_interrupted_s3_wait_closes_later_response(tmp_path, monkeypatch):
    release = Event()
    closed = Event()

    class Body(io.BytesIO):
        def close(self):
            super().close()
            closed.set()

    def read():
        release.wait(2)
        return {"Body": Body(b"manifest")}

    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    planner = commerce_batches.CommerceBatchPlanner(
        FakeS3Client(), bucket="lakehouse", state_path=tmp_path / "state.json",
    )
    monkeypatch.setattr(commerce_batches.Future, "result", interrupt)
    try:
        with planner.execution_budget(lambda: 1), pytest.raises(KeyboardInterrupt):
            planner._read_with_budget(read)
    finally:
        release.set()
    assert closed.wait(1)


@pytest.mark.parametrize("phase", ["planning", "stage", "query"])
def test_expired_run_budget_never_commits(monkeypatch, phase):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "2")
    now = 0
    planner = Planner()
    plan = planner.plan
    stages = []

    def planning(**kwargs):
        nonlocal now
        if phase == "planning":
            now = 3
        return plan(**kwargs)

    def stage(service, batch):
        nonlocal now
        stages.append(service)
        if phase == "stage":
            now = 3

    def query(sql):
        nonlocal now
        assert len(stages) == len(pipeline.COMMERCE_STAGES)
        if phase == "query":
            now = 3
        return gold(sql)

    monkeypatch.setattr(planner, "plan", planning)
    with pytest.raises(pipeline.CommercePipelineError, match="run time budget exhausted"):
        pipeline.run_commerce_batch(planner, query, run_stage=stage, clock=lambda: now)
    assert planner.commits == []
    assert planner.pending
    assert len(stages) == {"planning": 0, "stage": 1, "query": 8}[phase]


def test_compose_timeout_uses_remaining_shared_budget(monkeypatch):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "10")
    now = 0
    timeouts = []

    def run(command, **kwargs):
        nonlocal now
        timeouts.append(kwargs["timeout"])
        now += 1
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(pipeline.subprocess, "run", run)
    planner = Planner()
    pipeline.run_commerce_batch(
        planner, gold, clock=lambda: now,
        run_stage=lambda service, batch: pipeline.run_compose_stage(
            service, batch, bucket="unused",
        ),
    )
    assert timeouts == list(range(10, 2, -1))
    assert planner.commits == [BATCH["batch_id"]]


@pytest.mark.parametrize("expire_before_current", [True, False])
def test_full_snapshot_current_publication_obeys_budget(monkeypatch, expire_before_current):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "2")
    planner = Planner()
    monkeypatch.setattr(planner, "execution_options",
                        lambda _: {"customer_history_mode": "forward"}, raising=False)
    now = 0
    stages = []

    def stage(service, batch):
        nonlocal now
        stages.append(service)
        if service == (pipeline.COMMERCE_STAGES[-1] if expire_before_current
                       else pipeline.CURRENT_STAGE):
            now = 3

    with pytest.raises(pipeline.CommercePipelineError, match="run time budget exhausted"):
        pipeline.run_commerce_batch(planner, gold, run_stage=stage, clock=lambda: now)
    assert stages == [*pipeline.COMMERCE_STAGES, *(
        [] if expire_before_current else [pipeline.CURRENT_STAGE]
    )]
    assert planner.commits == [] and planner.pending


@pytest.mark.parametrize("value", ["0", "86401", "nan", "", "1.5"])
def test_invalid_run_budget_fails_before_planning(monkeypatch, value):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", value)
    planner = Planner()
    monkeypatch.setattr(planner, "plan", lambda **_: pytest.fail("must not plan"))
    with pytest.raises(pipeline.CommercePipelineError, match="COMMERCE_RUN_TIMEOUT_SECONDS"):
        pipeline.run_commerce_batch(planner, gold, run_stage=lambda *_: None)
