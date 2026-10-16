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


def completed_profile():
    return {"status": "ready", "samples": [{
        "batch_id": f"batch-{i}", "customer_history": {"status": "verified", "total_rows": rows,
                                                    "current_rows": 10000},
        "idle_replay": "verified", "run": {"durations_seconds": {"total": 150}},
        "stack_memory": {"peak_observed_bytes": 4000000000},
        "warehouse_storage": {"total_payload_bytes": 1000 * i, "reference_accounting": {
            "status": "reconciled", "total_payload_bytes": 1000 * i,
            "groups": {"current_data": {"bytes": 1000 * i}},
        }},
    } for i, rows in enumerate((10000, 20000, 20000, 30000), 1)]}


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
    for job in ("quality", "serving-integration", "ranger-integration", "lakehouse-integration",
                "release-readiness"):
        assert f"  {job}:" in Path(".github/workflows/ci.yml").read_text()
