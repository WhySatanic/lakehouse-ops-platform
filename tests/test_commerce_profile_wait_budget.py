import json
import subprocess

import pytest

from lakehouse_ops import commerce_pipeline_profile as profile


@pytest.mark.parametrize(("budget", "expected"), [(10, 45), (3600, 3635), (86400, 86435)])
def test_profile_waits_for_runner_budget_and_cleanup(tmp_path, monkeypatch, budget, expected):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", str(budget))

    def run(command, **kwargs):
        assert kwargs["timeout"] == expected
        kwargs["stdout"].write(json.dumps({"status": "idle"}))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(profile.subprocess, "run", run)
    assert profile.execute(["run-commerce-batch"], tmp_path, "runner")["status"] == "idle"


def test_invalid_runner_budget_starts_no_profile_child(tmp_path, monkeypatch):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "0")
    monkeypatch.setattr(profile.subprocess, "run", lambda *a, **kw: pytest.fail("no child"))
    with pytest.raises(RuntimeError, match="COMMERCE_RUN_TIMEOUT_SECONDS"):
        profile.execute(["run-commerce-batch"], tmp_path, "runner")


def test_fixture_landing_keeps_its_separate_wait(tmp_path, monkeypatch):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "10")

    def run(command, **kwargs):
        assert kwargs["timeout"] == 1800
        kwargs["stdout"].write("{}")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(profile.subprocess, "run", run)
    assert profile.execute(["land-commerce-fixture"], tmp_path, "landing") == {}
