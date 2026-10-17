from __future__ import annotations

import subprocess
from types import SimpleNamespace

import pytest

from lakehouse_ops import commerce_history_process as control


@pytest.mark.parametrize("failure", [None, "signal", "reap"])
def test_timeout_signals_created_group_before_reaping_and_preserves_exception(monkeypatch, failure):
    error = subprocess.TimeoutExpired(["profile"], 0.5)
    events = []

    class Process:
        pid = 12345
        waits = 0

        def wait(self, *, timeout):
            self.waits += 1
            events.append(("wait", timeout))
            if self.waits == 1 or failure == "reap":
                raise error
            return -9

    def popen(command, **options):
        assert options["start_new_session"] is True
        assert "timeout" not in options and "check" not in options
        return Process()

    def kill(group, sig):
        events.append(("kill", group, sig))
        if failure == "signal":
            raise PermissionError("simulated denied signal")

    monkeypatch.setattr(control, "_POSIX", True)
    monkeypatch.setattr(control.subprocess, "Popen", popen)
    monkeypatch.setattr(control.os, "killpg", kill, raising=False)
    with pytest.raises(subprocess.TimeoutExpired) as caught:
        control.run_profile_process(["profile"], timeout=0.5, check=True)
    assert caught.value is error
    assert events == [("wait", 0.5), ("kill", 12345, control._KILL), ("wait", 10)]
    assert error.profile_process_group["status"] == (
        "kill_requested_leader_reaped" if failure is None else "unconfirmed"
    )


@pytest.mark.parametrize("code,check", [(0, True), (7, False), (7, True)])
def test_normal_completion_and_nonzero_exit_keep_subprocess_semantics(monkeypatch, code, check):
    monkeypatch.setattr(control, "_POSIX", True)
    monkeypatch.setattr(control.subprocess, "Popen",
                        lambda *a, **kw: SimpleNamespace(pid=123, wait=lambda **kw: code))
    if code and check:
        with pytest.raises(subprocess.CalledProcessError) as caught:
            control.run_profile_process(["profile"], timeout=5, check=True)
        assert caught.value.returncode == code
    else:
        assert control.run_profile_process(["profile"], timeout=5, check=check).returncode == code


def test_non_posix_keeps_existing_runner_and_timeout(monkeypatch):
    error = subprocess.TimeoutExpired(["profile"], 0.5)

    def run(*args, **kwargs):
        assert kwargs == {"timeout": 0.5}
        raise error

    monkeypatch.setattr(control, "_POSIX", False)
    monkeypatch.setattr(control.subprocess, "run", run)
    with pytest.raises(subprocess.TimeoutExpired) as caught:
        control.run_profile_process(["profile"], timeout=0.5)
    assert caught.value is error and not hasattr(error, "profile_process_group")


def test_only_profile_module_uses_process_group_runner(monkeypatch):
    calls = []
    monkeypatch.setattr(control, "run_profile_process", lambda *a, **kw: calls.append("profile"))
    monkeypatch.setattr(control.subprocess, "run", lambda *a, **kw: calls.append("other"))
    control.run_experiment_command(["python", "-m", "lakehouse_ops.commerce_pipeline_profile"])
    control.run_experiment_command(["docker", "network", "inspect", "owned"])
    assert calls == ["profile", "other"]


@pytest.mark.skipif(
    not control._POSIX, reason="real POSIX group regression; also run in Linux Docker"
)
def test_actual_timeout_prevents_inherited_child_from_continuing(tmp_path):
    from integration.exercise_history_profile_process import exercise

    result = exercise(tmp_path / "process-tree")
    assert result["status"] == "verified" and result["child_observed"]
    assert result["timeout_preserved"] and not result["descendant_continued"]
    assert result["profile_process_group"]["status"] == "kill_requested_leader_reaped"
