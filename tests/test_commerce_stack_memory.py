import json
import subprocess

import pytest

from lakehouse_ops import commerce_pipeline_profile as profile


@pytest.mark.parametrize(("usage", "expected"), [("1GiB", 1024**3), ("2.5MiB", 2621440),
                                                ("3MB", 3000000), ("0B", 0)])
def test_project_memory_includes_core_and_one_off_containers(monkeypatch, usage, expected):
    calls = []

    def run(command, **kwargs):
        assert kwargs["check"] and kwargs["timeout"] == 10
        calls.append(command)
        if command[1] == "ps":
            assert command[3] == "label=com.docker.compose.project=lakehouse-ops-scale-test"
            return subprocess.CompletedProcess(command, 0, "core\nspark\n")
        assert command[-2:] == ["core", "spark"]
        body = "\n".join(json.dumps({"ID": name, "Name": name, "MemUsage": usage + " / 8GiB"})
                         for name in ("core", "spark"))
        return subprocess.CompletedProcess(command, 0, body)

    monkeypatch.setattr(profile.subprocess, "run", run)
    result = profile.stack_memory_sample("lakehouse-ops-scale-test")
    assert result["used_bytes"] == expected * 2
    assert len(result["containers"]) == 2 and len(calls) == 2


def test_empty_project_never_runs_unscoped_docker_stats(monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "")

    monkeypatch.setattr(profile.subprocess, "run", run)
    with pytest.raises(ValueError, match="no running"):
        profile.stack_memory_sample("lakehouse-ops-scale-test")
    assert len(calls) == 1
    with pytest.raises(ValueError, match="dedicated"):
        profile.stack_memory_sample("")
    assert len(calls) == 1


def test_observer_ends_on_pipeline_failure_and_retains_measurements(monkeypatch):
    monkeypatch.setattr(profile, "stack_memory_sample", lambda _: {
        "containers": [{"id": "owned", "used_bytes": 42}], "used_bytes": 42,
    })
    sampler = profile.StackMemorySampler("lakehouse-ops-scale-test")
    with pytest.raises(RuntimeError, match="pipeline failure"), sampler:
        raise RuntimeError("pipeline failure")
    assert not sampler.thread.is_alive()
    assert sampler.report()["peak_observed_bytes"] == 42


def test_failed_samples_are_not_reported_as_zero_memory(monkeypatch):
    def fail(_):
        raise subprocess.TimeoutExpired("docker", 10)

    monkeypatch.setattr(profile, "stack_memory_sample", fail)
    with profile.StackMemorySampler("lakehouse-ops-scale-test") as sampler:
        pass
    report = sampler.report()
    assert report["status"] == "partial" and report["peak_observed_bytes"] is None
    assert report["errors"] and report["samples"] == []
