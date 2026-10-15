"""Offline refusal and orchestration checks; these do not execute Spark."""
import json
import sys
import threading
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from integration import exercise_commerce_active_spark as drill

from lakehouse_ops.commerce_run_journal import CommerceRunJournal


def setup_probe(tmp_path, monkeypatch, *, state="running", changed=False, jobs=None):
    journal = CommerceRunJournal(tmp_path / "journal.sqlite3")
    batch = {"batch_id": "a" * 16, "manifest_sha256": "b" * 64}
    journal.record_container(drill.STAGE, "lakeops-commerce-" + "c" * 32, "launch_intent",
                             batch=batch, project_hint="lakehouse-ops-active-spark-test")
    calls = []
    count = 0

    def inspect(*args, **kwargs):
        nonlocal count
        count += 1
        return {"has_more": False, "containers": [{
            "service": drill.STAGE, "observation": state,
            "container_id": ("e" if changed and count > 1 else "d") * 64,
            "batch_id": batch["batch_id"], "manifest_sha256": batch["manifest_sha256"],
        }]}

    def run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout=json.dumps(jobs if jobs is not None else [
            {"status": "RUNNING", "numActiveTasks": 1, "application_id": "local-123"},
        ]))

    monkeypatch.setattr(drill, "inspect_containers", inspect)
    return journal, calls, run


def test_active_task_kills_only_rechecked_immutable_container(tmp_path, monkeypatch):
    journal, calls, run = setup_probe(tmp_path, monkeypatch)
    report = drill.kill_if_active(journal, "lakehouse-ops-active-spark-test", run=run)
    assert report["active_jobs"][0]["application_id"] == "local-123"
    assert report["container_id"] == "d" * 64
    assert calls[-1] == ["docker", "container", "kill", "--signal", "KILL", "--", "d" * 64]


@pytest.mark.parametrize("state", ["ownership_mismatch", "inspect_failed", "exited"])
def test_uncertain_ownership_or_liveness_never_probes_or_kills(tmp_path, monkeypatch, state):
    journal, calls, run = setup_probe(tmp_path, monkeypatch, state=state)
    with pytest.raises(ValueError, match="unconfirmed"):
        drill.kill_if_active(journal, "lakehouse-ops-active-spark-test", run=run)
    assert calls == []


@pytest.mark.parametrize("jobs", [[], [{"status": "SUCCEEDED", "numActiveTasks": 1}],
                                  [{"status": "RUNNING", "numActiveTasks": 0}],
                                  [{"status": "RUNNING", "numActiveTasks": True}],
                                  [{"status": "RUNNING", "numActiveTasks": 1}]])
def test_container_or_inactive_job_is_not_an_active_spark_boundary(tmp_path, monkeypatch, jobs):
    journal, calls, run = setup_probe(tmp_path, monkeypatch, jobs=jobs)
    assert drill.kill_if_active(journal, "lakehouse-ops-active-spark-test", run=run) is None
    assert len(calls) == 1


def test_name_reuse_after_spark_probe_refuses_kill(tmp_path, monkeypatch):
    journal, calls, run = setup_probe(tmp_path, monkeypatch, changed=True)
    with pytest.raises(ValueError, match="changed"):
        drill.kill_if_active(journal, "lakehouse-ops-active-spark-test", run=run)
    assert len(calls) == 1


@pytest.mark.parametrize("fails", [False, True])
def test_observer_and_failed_stage_both_required(monkeypatch, fails):
    observed = threading.Event()

    def probe(*args):
        observed.set()
        return {"container_id": "d" * 64}

    monkeypatch.setattr(drill, "kill_if_active", probe)

    def stage():
        assert observed.wait(5)
        if fails:
            raise drill.CommercePipelineError("container exited")

    evidence = {}
    with pytest.raises(drill.ActiveInterruption if fails else ValueError):
        drill.interrupt_stage(stage, None, "project", evidence)
    assert evidence["container_id"] == "d" * 64


def test_post_stage_failure_without_observed_spark_is_not_accepted(monkeypatch):
    monkeypatch.setattr(drill, "kill_if_active", lambda *a: None)

    def stage():
        raise drill.CommercePipelineError("unrelated failure")

    with pytest.raises(TimeoutError, match="not observed"):
        drill.interrupt_stage(stage, None, "project", {})


@pytest.mark.parametrize("failure", [None, "checkpoint", "oracle", "unobserved", "idle"])
def test_driver_retains_evidence_and_never_reports_unverified_recovery(
    tmp_path, monkeypatch, failure,
):
    output = tmp_path / "evidence"
    state = output / "checkpoint.json"
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "lakehouse-ops-active-spark-test")
    monkeypatch.setattr(sys, "argv", ["drill", "--output", str(output),
                                    "--s3-endpoint-url", "unused", "--server", "unused"])
    monkeypatch.setattr(drill, "commerce_runner_lock", nullcontext)

    class Trino:
        def __init__(self, *args, **kwargs):
            self.query = lambda sql: []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    monkeypatch.setattr(drill, "TrinoClient", Trino)
    monkeypatch.setattr(drill, "audit_tables", lambda *_: [])
    monkeypatch.setattr(drill.boto3, "client", lambda *a, **kw: None)
    monkeypatch.setattr(drill, "CommerceS3LandingZone", lambda *a, **kw: SimpleNamespace(
        write=lambda path: None,
    ))
    monkeypatch.setattr(drill, "CommerceBatchPlanner", lambda *a, **kw: None)
    monkeypatch.setattr(drill, "run_compose_stage", lambda *a, **kw: None)
    oracles = []

    def oracle(query, expected, index, batch_id):
        if failure == "oracle" and index == 1:
            raise ValueError("oracle mismatch")
        oracles.append(index)
        return {"batch_id": batch_id}

    monkeypatch.setattr(drill, "verify_snapshot", oracle)

    def interrupt(stage, journal, project, evidence):
        if failure == "unobserved":
            raise TimeoutError("no active Spark observed")
        evidence.update(container_id="d" * 64, manifest_sha256="b" * 64)
        if failure == "checkpoint":
            state.write_text("wrong checkpoint")
        raise drill.ActiveInterruption("observed")

    monkeypatch.setattr(drill, "interrupt_stage", interrupt)
    processed = []

    def pipeline(planner, query, *, run_stage, journal=None):
        if len(processed) == 3:
            if failure == "idle":
                run_stage(drill.STAGE, {})
            return {"status": "idle"}
        expected = json.loads((output / "scenario/expected.json").read_text())
        batch = expected["batches"][len(processed)]
        run_stage(drill.STAGE, batch)
        run_stage(drill.CURRENT_STAGE, batch)
        processed.append(batch["batch_id"])
        state.write_text(json.dumps(processed))
        return {"status": "ready"}

    monkeypatch.setattr(drill, "run_commerce_batch", pipeline)
    assert drill.main() == (0 if failure is None else 2)
    report = json.loads((output / "active-spark.json").read_text())
    assert report["status"] == ("ready" if failure is None else "failed")
    if failure is None:
        assert oracles == [0, 1, 2]
        assert report["checkpoint_before_sha256"] == report["checkpoint_after_failure_sha256"]
        assert report["interruption"]["run_id"] != report["samples"][1]["run_id"]
