from __future__ import annotations

import subprocess
import sys
from typing import Any

import httpx
import pytest

from lakehouse_ops import commerce_pipeline as pipeline
from lakehouse_ops.ingestion.commerce_batches import CommerceBatchError

BATCH = {"batch_id": "a" * 16, "manifest_sha256": "b" * 64}


class Planner:
    def __init__(self, pending: bool = True) -> None:
        self.pending = pending
        self.commits: list[str] = []

    def plan(self, *, max_batches: int) -> dict[str, Any]:
        assert max_batches == 1
        return {"batches": [BATCH] if self.pending else []}

    def commit(self, batch_id: str, *, expected_manifest_sha256: str) -> dict[str, Any]:
        assert expected_manifest_sha256 == BATCH["manifest_sha256"]
        self.commits.append(batch_id)
        self.pending = False
        return {"created": True, "batch_id": batch_id}


def gold(sql: str) -> list[dict[str, int]]:
    assert BATCH["batch_id"] in sql
    return [{"days": 2, "orders": 6, "captured_revenue_cents": 1000, "invalid_days": 0}]


def test_runs_ordered_jobs_then_verified_commit_and_idle_rerun() -> None:
    planner = Planner()
    stages = []

    def execute(service: str, batch: dict[str, str]) -> None:
        assert batch == BATCH
        assert planner.commits == []
        stages.append(service)

    def verify(sql: str) -> list[dict[str, int]]:
        assert stages == list(pipeline.COMMERCE_STAGES)
        assert planner.commits == []
        return gold(sql)

    report = pipeline.run_commerce_batch(planner, verify, run_stage=execute)
    assert report["status"] == "ready"
    assert report["completed_stages"] == list(pipeline.COMMERCE_STAGES)
    assert report["verification"]["orders"] == 6
    assert planner.commits == [BATCH["batch_id"]]
    idle = pipeline.run_commerce_batch(planner, verify, run_stage=execute)
    assert idle == {"status": "idle", "batch_id": None, "completed_stages": []}
    assert len(stages) == len(pipeline.COMMERCE_STAGES)


@pytest.mark.parametrize("failed_stage", pipeline.COMMERCE_STAGES)
def test_failed_stage_stops_pipeline_and_preserves_checkpoint(failed_stage: str) -> None:
    planner = Planner()
    stages = []

    def execute(service: str, batch: dict[str, str]) -> None:
        stages.append(service)
        if service == failed_stage:
            raise pipeline.CommercePipelineError(f"commerce stage failed: {service}")

    def forbidden(sql: str) -> list[dict[str, Any]]:
        pytest.fail("must not verify gold after a failed stage")

    with pytest.raises(pipeline.CommercePipelineError, match=failed_stage):
        pipeline.run_commerce_batch(planner, forbidden, run_stage=execute)
    assert planner.commits == []
    assert stages == list(pipeline.COMMERCE_STAGES[:stages.index(failed_stage) + 1])


@pytest.mark.parametrize("failure", ["empty", "transport", "protocol", "query"])
def test_gold_failure_preserves_checkpoint(failure: str) -> None:
    planner = Planner()

    def fail(sql: str) -> list[dict[str, Any]]:
        if failure == "transport":
            raise httpx.ConnectError("unavailable")
        if failure == "protocol":
            raise pipeline.TrinoProtocolError("malformed response")
        if failure == "query":
            raise pipeline.TrinoQueryError("invalid SQL")
        return [{"days": 0, "orders": 0, "captured_revenue_cents": 0, "invalid_days": 0}]

    with pytest.raises(pipeline.CommercePipelineError, match="verify-commerce-gold"):
        pipeline.run_commerce_batch(planner, fail, run_stage=lambda *_: None)
    assert planner.commits == []


def test_planning_failure_prevents_compute(monkeypatch: pytest.MonkeyPatch) -> None:
    planner = Planner()

    def fail(**kwargs: Any) -> dict[str, Any]:
        raise CommerceBatchError("invalid manifest")

    def forbidden(*args: Any) -> None:
        pytest.fail("must not start compute with an invalid plan")

    monkeypatch.setattr(planner, "plan", fail)
    with pytest.raises(CommerceBatchError, match="invalid manifest"):
        pipeline.run_commerce_batch(planner, gold, run_stage=forbidden)


def test_transient_gold_retry_does_not_repeat_compute() -> None:
    planner = Planner()
    stages = []
    statements = []

    def query(sql: str) -> list[dict[str, int]]:
        assert planner.commits == []
        statements.append(sql)
        if len(statements) == 1:
            raise httpx.ConnectError("coordinator restarting")
        return gold(sql)

    report = pipeline.run_commerce_batch(
        planner, query, run_stage=lambda service, batch: stages.append(service),
        attempts=3, delay_seconds=0,
    )
    assert report["status"] == "ready"
    assert len(statements) == 2
    assert statements[0] == statements[1]
    assert stages == list(pipeline.COMMERCE_STAGES)
    assert planner.commits == [BATCH["batch_id"]]


def test_runner_reports_monotonic_phase_durations_including_retry() -> None:
    now = 0.0
    statements = 0

    class TimedPlanner(Planner):
        def plan(self, *, max_batches: int) -> dict[str, Any]:
            nonlocal now
            now += 2
            return super().plan(max_batches=max_batches)

        def commit(self, batch_id: str, *, expected_manifest_sha256: str) -> dict[str, Any]:
            nonlocal now
            now += 3
            return super().commit(
                batch_id, expected_manifest_sha256=expected_manifest_sha256,
            )

    def execute(service: str, batch: dict[str, str]) -> None:
        nonlocal now
        assert service in pipeline.COMMERCE_STAGES
        assert batch == BATCH
        now += 1

    def query(sql: str) -> list[dict[str, int]]:
        nonlocal now, statements
        statements += 1
        now += 4 if statements == 1 else 6
        if statements == 1:
            raise httpx.ConnectError("coordinator restarting")
        return gold(sql)

    report = pipeline.run_commerce_batch(
        TimedPlanner(), query, run_stage=execute, attempts=2, delay_seconds=0,
        clock=lambda: now,
    )
    assert statements == 2
    assert report["durations_seconds"] == {
        "planning": 2, "stages": dict.fromkeys(pipeline.COMMERCE_STAGES, 1),
        "verification": 10, "checkpoint": 3, "total": 23,
    }


@pytest.mark.parametrize("failure", ["transport", "protocol", "query", "quality"])
def test_retry_failure_preserves_checkpoint_without_repeating_compute(failure: str) -> None:
    planner = Planner()
    stages = []
    statements = []

    def query(sql: str) -> list[dict[str, int]]:
        statements.append(sql)
        if failure == "transport":
            raise httpx.ReadTimeout("unavailable")
        if failure == "protocol":
            raise pipeline.TrinoProtocolError("invalid JSON")
        if failure == "query":
            raise pipeline.TrinoQueryError("invalid SQL")
        return [{"days": 0, "orders": 0, "captured_revenue_cents": 0, "invalid_days": 0}]

    with pytest.raises(pipeline.CommercePipelineError, match="verify-commerce-gold"):
        pipeline.run_commerce_batch(
            planner, query, run_stage=lambda service, batch: stages.append(service),
            attempts=3, delay_seconds=0,
        )
    assert len(statements) == (3 if failure == "transport" else 1)
    assert stages == list(pipeline.COMMERCE_STAGES)
    assert planner.commits == []


@pytest.mark.parametrize("pending", [True, False])
@pytest.mark.parametrize(
    ("attempts", "delay"), [(0, 0), (6, 0), (True, 0), (1, -1), (1, 61), (1, float("nan"))]
)
def test_invalid_retry_bounds_fail_before_planning(
    pending: bool, attempts: int, delay: float, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from lakehouse_ops.commerce_gold_gate import CommerceGoldGateError

    planner = Planner(pending)
    monkeypatch.setattr(planner, "plan", lambda **kwargs: pytest.fail("must not plan"))
    with pytest.raises(CommerceGoldGateError):
        pipeline.run_commerce_batch(
            planner, gold, run_stage=lambda *_: pytest.fail("must not compute"),
            attempts=attempts, delay_seconds=delay,
        )


@pytest.mark.parametrize("failure", [None, "process", "missing"])
def test_compose_adapter_binds_scope_and_keeps_stdout_for_json(
    monkeypatch: pytest.MonkeyPatch, failure: str | None,
) -> None:
    def run(command: list[str], **kwargs: Any) -> None:
        assert command[-4:] == ["run", "--rm", "--no-deps", "spark-commerce-bronze"]
        assert kwargs["env"]["COMMERCE_BATCH_ID"] == BATCH["batch_id"]
        assert kwargs["env"]["COMMERCE_MANIFEST_SHA256"] == BATCH["manifest_sha256"]
        assert kwargs["env"]["LAKEHOUSE_BUCKET"] == "selected-bucket"
        assert kwargs["stdout"] is sys.stderr
        assert kwargs["check"] is True
        if failure == "process":
            raise subprocess.CalledProcessError(1, command)
        if failure == "missing":
            raise FileNotFoundError("docker not found")

    monkeypatch.setattr(pipeline.subprocess, "run", run)
    if failure:
        with pytest.raises(pipeline.CommercePipelineError, match="spark-commerce-bronze"):
            pipeline.run_compose_stage("spark-commerce-bronze", BATCH, bucket="selected-bucket")
    else:
        pipeline.run_compose_stage("spark-commerce-bronze", BATCH, bucket="selected-bucket")
