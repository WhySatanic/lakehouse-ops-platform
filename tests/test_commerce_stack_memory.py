import json
import os
import subprocess
import uuid
from threading import Timer

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


@pytest.mark.parametrize("failure", [
    "retired", "daemon", "still-running", "retry-fails", "eof-retired",
    "eof-still-running", "eof-retry-fails", "eof-empty", "eof-two-retired",
    "eof-with-other-error",
])
def test_stats_exit_race_has_one_scoped_retry_and_keeps_other_failures(monkeypatch, failure):
    core, job = "a" * 12, "b" * 12
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if command[1] == "ps":
            assert command[3] == "label=com.docker.compose.project=lakehouse-ops-scale-test"
            ids = ([core, job] if len(calls) == 1
                   or failure in {"still-running", "eof-still-running"} else [core])
            if len(calls) != 1 and failure == "eof-empty":
                ids = []
            if len(calls) != 1 and failure == "eof-two-retired":
                ids = ["c" * 12]
            return subprocess.CompletedProcess(command, 0, "\n".join(ids))
        if len(calls) == 2 or failure in {"retry-fails", "eof-retry-fails"}:
            stderr = ("daemon unavailable" if failure == "daemon" else
                      f"Error response from daemon: No such container: {job}\n")
            if failure.startswith("eof-"):
                stderr = "EOF\n"
            if failure == "eof-with-other-error":
                stderr += "daemon unavailable\n"
            raise subprocess.CalledProcessError(1, command, stderr=stderr)
        assert command[-1] == core and job not in command
        return subprocess.CompletedProcess(command, 0, json.dumps({
            "ID": core, "Name": "core", "MemUsage": "1MiB / 8GiB",
        }))

    monkeypatch.setattr(profile.subprocess, "run", run)
    if failure in {"retired", "eof-retired"}:
        result = profile.stack_memory_sample("lakehouse-ops-scale-test")
        assert result["used_bytes"] == 1024**2
        assert result["resampled_after_container_exit"] == job
        assert len(calls) == 4
    else:
        with pytest.raises(subprocess.CalledProcessError):
            profile.stack_memory_sample("lakehouse-ops-scale-test")
        expected_calls = {
            "daemon": 2, "still-running": 3, "retry-fails": 4,
            "eof-still-running": 3, "eof-retry-fails": 4, "eof-empty": 3,
            "eof-two-retired": 3, "eof-with-other-error": 2,
        }
        assert len(calls) == expected_calls[failure]


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
@pytest.mark.parametrize("exit_timing", ["before-stats", "during-stats"])
def test_real_removed_stage_is_resampled_without_foreign_containers(monkeypatch, exit_timing):
    project = f"lakehouse-ops-scale-memory-{uuid.uuid4().hex}"
    real_run = subprocess.run
    owned = []
    stopper = None
    stop_errors = []
    stats_error = None
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

        def stop_stage():
            command = (["docker", "kill", owned[1]] if exit_timing == "during-stats"
                       else ["docker", "stop", "--time", "1", owned[1]])
            result = real_run(command,
                              capture_output=True, timeout=10)
            if result.returncode:
                stop_errors.append(result.stderr)

        def run(command, **kwargs):
            nonlocal removed, stopper, stats_error
            if command[1] == "stats" and not removed and exit_timing == "during-stats":
                # Keep the job alive until the stats request starts, then retire it in flight.
                removed = True
                stopper = Timer(0.5, stop_stage)
                stopper.start()
            try:
                result = real_run(command, **kwargs)
            except subprocess.CalledProcessError as error:
                stats_error = error.stderr
                raise
            if command[1] == "ps" and not removed and exit_timing == "before-stats":
                assert set(result.stdout.split()) == {container[:12] for container in owned}
                stop_stage()
                removed = True
            return result

        monkeypatch.setattr(profile.subprocess, "run", run)
        sample = profile.stack_memory_sample(project)
        if stopper:
            stopper.join(timeout=15)
            assert not stopper.is_alive()
            assert stats_error.strip() == "EOF"
        assert stop_errors == []
        assert removed and sample["resampled_after_container_exit"] == owned[1][:12]
        assert [row["id"] for row in sample["containers"]] == [owned[0][:12]]
        assert sample["used_bytes"] > 0
        print(json.dumps({"project": project, "status": "verified", "exit_timing": exit_timing,
                          "stats_error": stats_error, **sample}))
    finally:
        if stopper:
            stopper.join(timeout=15)
        for container in owned:
            real_run(["docker", "rm", "-f", container],
                     capture_output=True, timeout=10, check=False)
