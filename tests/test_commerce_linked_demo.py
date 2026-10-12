from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from lakehouse_ops.commerce_linked_demo import SERVICES, demo, free_ports


def checkout(root):
    source = root / "src/lakehouse_ops/commerce_linked_acceptance.py"
    source.parent.mkdir(parents=True)
    source.touch()
    (source.parent / "commerce_owner_exit.py").touch()
    (source.parent / "commerce_pipeline_profile.py").touch()
    (root / "compose.spark-memory.yaml").touch()


@pytest.mark.parametrize("failure", [None, "docker", "spark-image", "config", "start",
                                    "linked", "stop",
                                    "timeout", "launch", "bad_report", "absent_report",
                                    "malformed_report"])
@pytest.mark.parametrize("scenario", ["linked", "owner-exit", "profile"])
def test_scoped_demo_retains_failure_and_always_stops_started_stack(tmp_path: Path, failure,
                                                                  monkeypatch, scenario):
    checkout(tmp_path)
    output = tmp_path / "evidence"
    commands, projects = [], []
    monkeypatch.setenv("COMPOSE_FILE", "unrelated-compose.yaml")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "unrelated-token")
    original = dict(os.environ)

    def run(command, **kwargs):
        name = ("docker" if command[1] == "info" else
                "spark-image" if command[1] == "image" else
                "linked" if command[1] == "-m" else
                "config" if "config" in command else
                "start" if "up" in command else "stop")
        commands.append(name)
        project = kwargs["env"]["COMPOSE_PROJECT_NAME"]
        projects.append(project)
        prefix = "scale" if scenario == "profile" else scenario
        assert project.startswith(f"lakehouse-ops-{prefix}-demo-")
        assert kwargs["cwd"] == tmp_path and kwargs["timeout"] > 0
        expected_files = [str(tmp_path / "compose.yaml")]
        if scenario == "profile":
            expected_files.append(str(tmp_path / "compose.spark-memory.yaml"))
            assert kwargs["env"]["COMPOSE_PATH_SEPARATOR"] == os.pathsep
        assert kwargs["env"]["COMPOSE_FILE"] == os.pathsep.join(expected_files)
        assert "AWS_SESSION_TOKEN" not in kwargs["env"]
        if command[:2] == ["docker", "compose"]:
            assert command[command.index("--project-name") + 1] == project
            assert not any(arg in command for arg in ("down", "rm", "--volumes", "build"))
        if name == "start":
            assert command[-len(SERVICES):] == list(SERVICES)
            assert "--no-build" in command and command[command.index("--pull") + 1] == "never"
        if name == "stop":
            assert command[-len(SERVICES):] == list(SERVICES)
        if name == "linked":
            expected = {"linked": "commerce_linked_acceptance", "owner-exit": "commerce_owner_exit",
                        "profile": "commerce_pipeline_profile"}[scenario]
            assert command[2] == f"lakehouse_ops.{expected}"
            expected_timeout = {"linked": 1800, "owner-exit": 6600, "profile": 3600}[scenario]
            assert kwargs["timeout"] == expected_timeout
            if scenario == "profile":
                assert command[-3:] == ["--orders", "100", "1000"]
            else:
                assert "--orders" not in command
            if failure == "timeout":
                raise subprocess.TimeoutExpired(command, kwargs["timeout"])
            if failure == "launch":
                raise FileNotFoundError("cannot launch child")
            if failure != "absent_report":
                acceptance = output / "acceptance"
                acceptance.mkdir()
                retained = {"linked": "linked.json", "owner-exit": "owner-exit.json",
                            "profile": "profile.json"}[scenario]
                (acceptance / retained).write_text(json.dumps({
                    "status": "failed" if failure == "bad_report" else "ready",
                }))
                if failure == "malformed_report":
                    (acceptance / retained).write_text("{invalid")
        return SimpleNamespace(returncode=1 if failure == name else 0)

    report = demo(tmp_path, output, run=run, scenario=scenario)
    assert report["status"] == ("ready" if failure is None else "failed")
    assert len(set(projects)) == 1
    assert ("stop" in commands) == (failure not in ("docker", "spark-image", "config"))
    assert all("elapsed_seconds" in step for step in report["steps"])
    assert json.loads((output / "demo.json").read_text()) == report
    assert dict(os.environ) == original


def test_unknown_scenario_cannot_start_or_mutate(tmp_path):
    with pytest.raises(ValueError, match="unsupported"):
        demo(tmp_path, tmp_path / "absent", scenario="unrelated")
    assert not (tmp_path / "absent").exists()


def test_port_probe_is_distinct_and_releases_sockets():
    ports = free_ports()
    assert len(set(ports.values())) == 4
    assert all(1024 <= int(port) <= 65535 for port in ports.values())


def test_no_existing_output_or_unrelated_root_mutation(tmp_path: Path):
    with pytest.raises(ValueError, match="checkout"):
        demo(tmp_path, tmp_path / "absent")
    assert not (tmp_path / "absent").exists()
    checkout(tmp_path)
    output = tmp_path / "evidence"
    output.mkdir()
    retained = output / "demo.json"
    retained.write_text("existing")
    with pytest.raises(ValueError, match="empty"):
        demo(tmp_path, output)
    assert retained.read_text() == "existing"


@pytest.mark.parametrize("status,code", [("ready", 0), ("failed", 2)])
def test_entrypoint_returns_demo_outcome(tmp_path: Path, monkeypatch, capsys, status, code):
    from lakehouse_ops import commerce_linked_demo

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["demo", "--output", "evidence"])
    monkeypatch.setattr(commerce_linked_demo, "demo", lambda *_: {"status": status})
    assert commerce_linked_demo.main() == code
    assert json.loads(capsys.readouterr().out) == {"status": status}


def test_entrypoint_rejects_unrelated_root(tmp_path: Path, monkeypatch):
    from lakehouse_ops.commerce_linked_demo import main

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["demo", "--output", "evidence"])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2
