from __future__ import annotations

import subprocess

import pytest
from test_commerce_pipeline import BATCH, Planner, gold

from lakehouse_ops import commerce_pipeline as pipeline


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
