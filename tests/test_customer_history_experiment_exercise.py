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


def test_profile_group_timeout_evidence_survives_experiment_cleanup(tmp_path):
    exercise = module()
    output = tmp_path / "group-timeout"
    error = subprocess.TimeoutExpired(["profile"], 1800)
    error.profile_process_group = {
        "process_group_id": 12345, "status": "kill_requested_leader_reaped",
        "scope": "profile_controller_group_not_Docker_stages_or_commit_outcome",
    }
    calls = []

    def run(command, **options):
        calls.append(command)
        if command[1:3] == ["-m", "lakehouse_ops.commerce_pipeline_profile"]:
            raise error
        if command[1:3] == ["network", "inspect"]:
            return SimpleNamespace(returncode=0, stdout=json.dumps([{
                "Name": command[-1], "Containers": {},
                "Labels": {"com.docker.compose.project": options["env"]["COMPOSE_PROJECT_NAME"]},
            }]))
        return SimpleNamespace(returncode=0)

    with pytest.raises(subprocess.TimeoutExpired) as caught:
        exercise["experiment"](tmp_path, output, run=run)
    assert caught.value is error
    report = json.loads((output / "experiment.json").read_text())
    profile = next(step for step in report["steps"] if step["name"] == "profile")
    assert profile["status"] == "failed" and report["status"] == "failed"
    assert profile["process_group"] == error.profile_process_group
    assert "measurements" not in report
    assert any("down" in command for command in calls)
