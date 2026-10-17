from __future__ import annotations

import copy
import json
import runpy
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest


def module():
    return runpy.run_path(str(Path(__file__).parent / "integration" /
                               "test_customer_history_experiment.py"))


def completed_profile(batches=4):
    totals = ((10000, 20000, 20000, 30000) if batches == 4
              else range(10000, (batches + 1) * 10000, 10000))
    return {"status": "ready", "samples": [{
        "batch_id": f"batch-{i}", "customer_history": {"status": "verified", "total_rows": rows,
                                                    "current_rows": 10000},
        "idle_replay": "verified", "run": {"durations_seconds": {"total": 150}},
        "stack_memory": {"peak_observed_bytes": 4000000000},
        "warehouse_storage": {"total_payload_bytes": 1000 * i, "reference_accounting": {
            "status": "reconciled", "total_payload_bytes": 1000 * i,
            "groups": {"current_data": {"bytes": 1000 * i}},
        }},
    } for i, rows in enumerate(totals, 1)]}


@pytest.mark.parametrize("fault", [None, "partial", "wrong_last_period_count", "running"])
@pytest.mark.parametrize("batches", [50, 100])
def test_depth_measurements_require_every_observed_snapshot(fault, batches):
    profile = completed_profile(batches)
    if fault == "partial":
        profile["samples"] = profile["samples"][:32]
    elif fault == "wrong_last_period_count":
        profile["samples"][-1]["customer_history"]["total_rows"] -= 10000
    elif fault == "running":
        profile["status"] = "running"
    if fault:
        with pytest.raises(ValueError):
            module()["measurements"](profile, batches=batches)
    else:
        result = module()["measurements"](profile, batches=batches)
        assert len(result) == batches
        assert [sample["history_rows"] for sample in result] == list(
            range(10000, (batches + 1) * 10000, 10000)
        )


@pytest.mark.parametrize("batches", [0, 5, 101, True, 4.0, "50"])
def test_invalid_depth_rejected_before_output_or_docker(tmp_path, batches):
    output = tmp_path / "invalid"

    def run(*args, **kwargs):
        pytest.fail("invalid depth must not start Docker")

    with pytest.raises(ValueError):
        module()["experiment"](tmp_path, output, run=run, batches=batches)
    assert not output.exists()


@pytest.mark.parametrize("fault", [None, "profile_timeout", "cleanup"])
@pytest.mark.parametrize("batches,timeout", [(50, 10800), (100, 19500)])
def test_depth_experiment_bounds_child_and_preserves_failure(tmp_path, fault, batches, timeout):
    output = tmp_path / "depth"
    calls = []

    def run(command, **options):
        calls.append(command)
        if command[1] == "-m":
            assert options["timeout"] == timeout
            assert command[command.index("--batches") + 1] == str(batches)
            begin = command.index("--customer-revisions") + 1
            end = command.index("--warehouse-inventory")
            assert command[begin:end] == [str(i) for i in range(batches)]
            (output / "profile").mkdir()
            if fault == "profile_timeout":
                partial = completed_profile(batches)
                partial.update(status="failed", samples=partial["samples"][:32])
                (output / "profile/profile.json").write_text(json.dumps(partial))
                raise subprocess.TimeoutExpired(command, options["timeout"])
            (output / "profile/profile.json").write_text(json.dumps(completed_profile(batches)))
        if "down" in command and fault == "cleanup":
            raise subprocess.TimeoutExpired(command, options["timeout"])
        assert "--volumes" not in command and "prune" not in command
        if command[1:3] == ["network", "inspect"]:
            project = options["env"]["COMPOSE_PROJECT_NAME"]
            return SimpleNamespace(returncode=0, stdout=json.dumps([{
                "Name": command[-1], "Containers": {},
                "Labels": {"com.docker.compose.project": project},
            }]))
        return SimpleNamespace(returncode=0)

    if fault == "profile_timeout":
        with pytest.raises(subprocess.TimeoutExpired):
            module()["experiment"](tmp_path, output, run=run, batches=batches)
    else:
        result = module()["experiment"](tmp_path, output, run=run, batches=batches)
        assert result["status"] == ("failed" if fault else "ready")
    evidence = json.loads((output / "experiment.json").read_text())
    assert evidence["workload"]["batches"] == batches
    assert evidence["workload"]["customer_revisions"] == list(range(batches))
    assert evidence["status"] == ("failed" if fault else "ready")
    assert any("down" in command for command in calls)
    if fault == "profile_timeout":
        assert "measurements" not in evidence
        assert len(json.loads((output / "profile/profile.json").read_text())["samples"]) == 32


@pytest.mark.parametrize("fault", [None, "rows", "current", "sum", "idle", "partial"])
def test_measurement_summary_requires_literal_growth_and_reconciled_payload(fault):
    profile = completed_profile()
    if fault in {"rows", "current"}:
        profile["samples"][2]["customer_history"][
            "total_rows" if fault == "rows" else "current_rows"
        ] += 1
    elif fault == "sum":
        profile["samples"][0]["warehouse_storage"]["total_payload_bytes"] += 1
    elif fault == "idle":
        profile["samples"][0]["idle_replay"] = "failed"
    elif fault == "partial":
        profile["samples"].pop()
    if fault:
        with pytest.raises(ValueError):
            module()["measurements"](profile)
    else:
        original = copy.deepcopy(profile)
        result = module()["measurements"](profile)
        assert [sample["history_rows"] for sample in result] == [10000, 20000, 20000, 30000]
        assert [sample["warehouse_payload_bytes"] for sample in result] == [1000, 2000, 3000, 4000]
        assert profile == original


@pytest.mark.parametrize("fault", [None, "start", "observer", "profile", "cleanup"])
def test_experiment_setup_failure_and_timeout_keep_evidence_and_scoped_cleanup(tmp_path, fault):
    exercise = module()
    output = tmp_path / "evidence"
    calls = []

    def run(command, **options):
        calls.append(command)
        project = options["env"]["COMPOSE_PROJECT_NAME"]
        assert project.startswith("lakehouse-ops-scale-history-")
        if command[1] == "-m":
            label = "profile"
            assert options["timeout"] == 1800
            assert "--warehouse-inventory" in command and "--retained-references" in command
            profile = output / "profile"
            profile.mkdir()
            (profile / "profile.json").write_text(json.dumps(completed_profile()))
        else:
            label = ("cleanup" if "down" in command else "start" if "up" in command
                     else "observer" if "run" in command else "logs")
            if "compose" in command:
                assert command[command.index("--project-name") + 1] == project
            assert "--volumes" not in command and "prune" not in command
        if label == fault:
            raise subprocess.TimeoutExpired(command, options["timeout"])
        if command[1:3] == ["network", "inspect"]:
            return SimpleNamespace(returncode=0, stdout=json.dumps([{
                "Name": command[-1], "Containers": {},
                "Labels": {"com.docker.compose.project": project},
            }]))
        return SimpleNamespace(returncode=0)

    if fault and fault != "cleanup":
        with pytest.raises(subprocess.TimeoutExpired):
            exercise["experiment"](tmp_path, output, run=run)
    else:
        result = exercise["experiment"](tmp_path, output, run=run)
        assert result["status"] == ("failed" if fault else "ready")
    evidence = json.loads((output / "experiment.json").read_text())
    assert evidence["status"] == ("failed" if fault else "ready")
    assert any("down" in command for command in calls)
    if fault == "observer":
        assert not any(command[1] == "-m" for command in calls)


def test_ci_experiment_preserves_required_ci_and_uploads_failure_evidence():
    workflow = Path(".github/workflows/customer-history-experiment.yml").read_text()
    assert "workflow_dispatch:" in workflow and "pull_request:" in workflow
    assert "LAKEOPS_RUN_CUSTOMER_HISTORY_EXPERIMENT=1" in workflow
    assert "if: always()" in workflow and "artifacts/customer-history-ci/" in workflow
    assert "contents: read" in workflow and "secrets." not in workflow
    assert 'default: "4"' in workflow and '          - "50"' in workflow
    assert '          - "100"' in workflow
    assert ("${{ inputs.snapshots == '100' && 360 || "
            "inputs.snapshots == '50' && 220 || 60 }}") in workflow
    assert ("${{ inputs.snapshots == '100' && 335 || "
            "inputs.snapshots == '50' && 190 || 38 }}") in workflow
    assert "LAKEOPS_CUSTOMER_HISTORY_BATCHES: ${{ inputs.snapshots || '4' }}" in workflow
    for job in ("quality", "serving-integration", "ranger-integration", "lakehouse-integration",
                "release-readiness"):
        assert f"  {job}:" in Path(".github/workflows/ci.yml").read_text()


@pytest.mark.parametrize("inspection_fault", [None, "daemon", "ownership"])
def test_profile_timeout_retains_read_only_container_evidence_before_teardown(
    tmp_path, inspection_fault,
):
    from lakehouse_ops.commerce_run_journal import CommerceRunJournal

    exercise = module()
    output = tmp_path / "failure"
    calls = []
    state = {}
    error = subprocess.TimeoutExpired(["profile-child"], 1800)

    def run(command, **options):
        calls.append(command)
        project = options["env"]["COMPOSE_PROJECT_NAME"]
        if command[1] == "-m":
            profile = output / "profile"
            profile.mkdir()
            (profile / "checkpoint.json.active.json").write_text('{"active": true}')
            journal = CommerceRunJournal(profile / "checkpoint.json.runs.sqlite3")
            journal.record_container("spark-commerce-order-silver", "owned-stage", "launch_intent",
                                     batch={"batch_id": "a" * 16, "manifest_sha256": "b" * 64},
                                     project_hint=project)
            state.update(journal=journal, bytes=journal.path.read_bytes())
            raise error
        if command[:3] == ["docker", "container", "inspect"]:
            assert command == ["docker", "container", "inspect", "--", "owned-stage"]
            assert options["timeout"] == 5
            if inspection_fault == "daemon":
                raise subprocess.TimeoutExpired(command, 5)
            return SimpleNamespace(returncode=0, stdout=json.dumps([{
                "Name": "/owned-stage", "Id": "d" * 64, "State": {"Status": "running"},
                "Config": {"Env": ["SECRET=must-not-be-persisted"], "Labels": {
                    "lakehouse.ops.run_id": state["journal"].run_id,
                    "lakehouse.ops.batch_id": "a" * 16,
                    "lakehouse.ops.manifest_sha256": "b" * 64,
                    "com.docker.compose.project": "foreign" if inspection_fault == "ownership"
                                                    else project,
                    "com.docker.compose.service": "spark-commerce-order-silver",
                }},
            }]))
        if command[1:3] == ["network", "inspect"]:
            return SimpleNamespace(returncode=0, stdout=json.dumps([{
                "Name": command[-1], "Containers": {"d" * 64: {}},
                "Labels": {"com.docker.compose.project": project},
            }]))
        return SimpleNamespace(returncode=0)

    with pytest.raises(subprocess.TimeoutExpired) as caught:
        exercise["experiment"](tmp_path, output, run=run)
    assert caught.value is error
    report = json.loads((output / "experiment.json").read_text())
    observed = report["failure_containers"]
    assert observed == json.loads((output / "containers-at-failure.json").read_text())
    assert observed["limit"] == 3
    item = observed["containers"][0]
    assert item["observation"] == {None: "running", "daemon": "inspect_failed",
                                    "ownership": "ownership_mismatch"}[inspection_fault]
    if inspection_fault is None:
        assert item["container_id"] == "d" * 64
    else:
        assert "container_id" not in item
    assert state["journal"].path.read_bytes() == state["bytes"]
    assert (output / "profile/checkpoint.json.active.json").read_text() == '{"active": true}'
    inspected = next(i for i, c in enumerate(calls) if c[:3] == ["docker", "container", "inspect"])
    stopped = next(i for i, c in enumerate(calls) if "down" in c)
    assert inspected < stopped
    assert "SECRET" not in json.dumps(report)
    assert not any(c[:2] == ["docker", "rm"] or "kill" in c or "prune" in c for c in calls)


@pytest.mark.parametrize("write_failure", [False, True])
def test_missing_journal_and_diagnostic_write_failure_keep_original_timeout(
    tmp_path, monkeypatch, write_failure,
):
    exercise = module()
    output = tmp_path / "missing-journal"
    error = subprocess.TimeoutExpired(["profile-child"], 1800)
    write_text = Path.write_text

    def write(path, *args, **kwargs):
        if write_failure and path.name == "containers-at-failure.json":
            raise OSError("simulated diagnostic write failure")
        return write_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", write)

    def run(command, **options):
        if command[1] == "-m":
            raise error
        if command[1:3] == ["network", "inspect"]:
            return SimpleNamespace(returncode=1, stderr=f"network {command[-1]} not found")
        assert command[:3] != ["docker", "container", "inspect"]
        return SimpleNamespace(returncode=0)

    with pytest.raises(subprocess.TimeoutExpired) as caught:
        exercise["experiment"](tmp_path, output, run=run)
    assert caught.value is error
    report = json.loads((output / "experiment.json").read_text())
    assert report["failure_containers"]["status"] == "incomplete"
    assert not (output / "profile/checkpoint.json.runs.sqlite3").exists()
    if write_failure:
        assert report["failure_containers_write_error"] == "OSError"


def test_timeout_smoke_refuses_existing_output_before_docker(tmp_path, monkeypatch):
    smoke = runpy.run_path(str(Path(__file__).parent / "integration" /
                              "exercise_customer_history_timeout.py"))
    evidence = tmp_path / "timeout-smoke.json"
    evidence.write_bytes(b"existing")

    def forbidden(*args, **kwargs):
        pytest.fail("existing output must be refused before Docker")

    monkeypatch.setattr(subprocess, "run", forbidden)
    with pytest.raises(ValueError, match="new evidence"):
        smoke["exercise"](tmp_path)
    assert evidence.read_bytes() == b"existing"
