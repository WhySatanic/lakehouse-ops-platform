from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from lakehouse_ops.commerce_local_check import check, main


def checkout(root: Path):
    source = root / "src/lakehouse_ops/commerce_pipeline.py"
    source.parent.mkdir(parents=True)
    source.touch()


def test_all_gates_and_real_smokes_are_required(tmp_path: Path, monkeypatch):
    checkout(tmp_path)
    output = tmp_path / "evidence"
    commands = []
    monkeypatch.setenv("LAKEOPS_RUN_S3_SMOKE", "1")
    monkeypatch.setenv("PYTEST_ADDOPTS", "--collect-only")

    def run(command, **kwargs):
        commands.append(command)
        assert kwargs["cwd"] == tmp_path
        assert kwargs["timeout"] > 0
        assert "LAKEOPS_RUN_S3_SMOKE" not in kwargs["env"]
        assert "PYTEST_ADDOPTS" not in kwargs["env"]
        assert kwargs["env"]["COMPOSE_PROJECT_NAME"].startswith("lakehouse-ops-local-check-")
        if "tests/integration/test_spark_memory.py" in command:
            assert kwargs["env"]["LAKEOPS_RUN_SPARK_MEMORY_SMOKE"] == "1"
            assert kwargs["env"]["LAKEOPS_RUN_DOCKER_TIMEOUT_SMOKE"] == "1"
            (output / "docker-smoke.xml").write_text(
                '<testsuites><testsuite><testcase/><testcase/></testsuite></testsuites>',
            )
        else:
            assert "LAKEOPS_RUN_SPARK_MEMORY_SMOKE" not in kwargs["env"]
        return SimpleNamespace(returncode=0)

    report = check(tmp_path, output, run=run)
    assert report["status"] == "ready"
    assert len(commands) == 8
    assert all(step["status"] == "ready" for step in report["steps"])
    assert all(step["elapsed_seconds"] >= 0 for step in report["steps"])
    assert json.loads((output / "check.json").read_text()) == report


@pytest.mark.parametrize("failure", ["exit", "launch", "timeout"])
def test_failure_stops_and_retains_evidence(tmp_path: Path, failure):
    checkout(tmp_path)
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if failure == "launch":
            raise FileNotFoundError("Docker unavailable")
        if failure == "timeout":
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        return SimpleNamespace(returncode=1)

    report = check(tmp_path, tmp_path / "evidence", run=run)
    assert report["status"] == "failed"
    assert len(calls) == 1
    assert report["steps"][0]["status"] == "failed"
    assert "elapsed_seconds" in report["steps"][0]


@pytest.mark.parametrize("xml", ["invalid", "<testsuites/>",
                                '<testsuites><testcase/><testcase><skipped/></testcase>'
                                '</testsuites>'])
def test_skipped_missing_or_invalid_smoke_evidence_is_not_success(tmp_path: Path, xml):
    checkout(tmp_path)
    output = tmp_path / "evidence"

    def run(command, **kwargs):
        (output / "docker-smoke.xml").write_text(xml)
        return SimpleNamespace(returncode=0)

    assert check(tmp_path, output, run=run)["status"] == "failed"


def test_reject_nonempty_output_without_overwrite(tmp_path: Path):
    checkout(tmp_path)
    output = tmp_path / "evidence"
    output.mkdir()
    retained = output / "check.json"
    retained.write_text("existing")
    with pytest.raises(ValueError, match="empty"):
        check(tmp_path, output)
    assert retained.read_text() == "existing"


def test_main_rejects_unrelated_checkout(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["check", "--output", str(tmp_path / "evidence")])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2
    assert not (tmp_path / "evidence").exists()


@pytest.mark.parametrize("status,code", [("ready", 0), ("failed", 2)])
def test_main_returns_report_outcome(tmp_path: Path, monkeypatch, capsys, status, code):
    import lakehouse_ops.commerce_local_check as checker

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["check", "--output", "evidence"])
    monkeypatch.setattr(checker, "check", lambda *_: {"status": status})
    assert checker.main() == code
    assert json.loads(capsys.readouterr().out) == {"status": status}
