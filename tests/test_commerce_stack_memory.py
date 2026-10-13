import json
import os
import subprocess
import uuid

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


@pytest.mark.parametrize("failure", ["retired", "daemon", "still-running", "retry-fails"])
def test_stats_exit_race_has_one_scoped_retry_and_keeps_other_failures(monkeypatch, failure):
    core, job = "a" * 12, "b" * 12
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if command[1] == "ps":
            assert command[3] == "label=com.docker.compose.project=lakehouse-ops-scale-test"
            ids = [core, job] if len(calls) == 1 or failure == "still-running" else [core]
            return subprocess.CompletedProcess(command, 0, "\n".join(ids))
        if len(calls) == 2 or failure == "retry-fails":
            stderr = ("daemon unavailable" if failure == "daemon" else
                      f"Error response from daemon: No such container: {job}\n")
            raise subprocess.CalledProcessError(1, command, stderr=stderr)
        assert command[-1] == core and job not in command
        return subprocess.CompletedProcess(command, 0, json.dumps({
            "ID": core, "Name": "core", "MemUsage": "1MiB / 8GiB",
        }))

    monkeypatch.setattr(profile.subprocess, "run", run)
    if failure == "retired":
        result = profile.stack_memory_sample("lakehouse-ops-scale-test")
        assert result["used_bytes"] == 1024**2
        assert result["resampled_after_container_exit"] == job
        assert len(calls) == 4
    else:
        with pytest.raises(subprocess.CalledProcessError):
            profile.stack_memory_sample("lakehouse-ops-scale-test")
        assert len(calls) == {"daemon": 2, "still-running": 3, "retry-fails": 4}[failure]


def test_memory_sample_rejects_foreign_id_with_matching_count(monkeypatch):
    def run(command, **kwargs):
        if command[1] == "ps":
            return subprocess.CompletedProcess(command, 0, "owned")
        return subprocess.CompletedProcess(command, 0, json.dumps({
            "ID": "foreign", "Name": "foreign", "MemUsage": "1MiB / 8GiB",
        }))

    monkeypatch.setattr(profile.subprocess, "run", run)
    with pytest.raises(ValueError, match="changed"):
        profile.stack_memory_sample("lakehouse-ops-scale-test")


@pytest.mark.skipif(os.getenv("RUN_DOCKER_STACK_MEMORY_TEST") != "1",
                    reason="requires local Docker and the existing Spark image")
def test_real_removed_stage_is_resampled_without_foreign_containers(monkeypatch):
    project = f"lakehouse-ops-scale-memory-{uuid.uuid4().hex}"
    real_run = subprocess.run
    owned = []
    try:
        for role in ("core", "stage"):
            result = real_run([
                "docker", "run", "-d", "--rm", "--pull", "never", "--network", "none",
                "--read-only", "--memory", "64m", "--name", f"{project}-{role}",
                "--label", f"com.docker.compose.project={project}",
                "--entrypoint", "/bin/sh", "lakehouse-ops/spark-iceberg:3.5.9-1.11.0",
                "-c", "sleep 60",
            ], check=True, capture_output=True, text=True, timeout=15)
            owned.append(result.stdout.strip())

        removed = False

        def run(command, **kwargs):
            nonlocal removed
            result = real_run(command, **kwargs)
            if command[1] == "ps" and not removed:
                assert set(result.stdout.split()) == {container[:12] for container in owned}
                real_run(["docker", "stop", "--time", "1", owned[1]],
                         check=True, capture_output=True, timeout=10)
                removed = True
            return result

        monkeypatch.setattr(profile.subprocess, "run", run)
        sample = profile.stack_memory_sample(project)
        assert removed and sample["resampled_after_container_exit"] == owned[1][:12]
        assert [row["id"] for row in sample["containers"]] == [owned[0][:12]]
        assert sample["used_bytes"] > 0
        print(json.dumps({"project": project, "status": "verified", **sample}))
    finally:
        for container in owned:
            real_run(["docker", "rm", "-f", container],
                     capture_output=True, timeout=10, check=False)
