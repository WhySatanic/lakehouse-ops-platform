from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops import local_quality as quality


@pytest.fixture
def root(tmp_path: Path) -> Path:
    (tmp_path / "pyproject.toml").touch()
    (tmp_path / "compose.yaml").touch()
    return tmp_path


def test_complete_quality_sequence_is_read_only_and_retains_results(root: Path) -> None:
    calls = []
    ticks = iter(range(20))

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[Any]:
        calls.append(command)
        assert kwargs == {"cwd": root, "check": False, "stdout": sys.stderr, "timeout": 300}
        return subprocess.CompletedProcess(command, 0)

    report = quality.run_quality(root, run=run, clock=lambda: next(ticks))
    assert report["status"] == "ready"
    assert calls == [command for _, command in quality.CHECKS]
    assert all(row["elapsed_seconds"] == 1 for row in report["checks"])
    assert all(row["status"] == "passed" for row in report["checks"])
    assert report["scope"] == "local_quality_not_integration"
    compose = next(command for name, command in quality.CHECKS if name == "compose")
    assert compose[-2:] == ["config", "--quiet"]
    assert next(command for name, command in quality.CHECKS if name == "tests")[-1] == "-q"
    assert "--no-cov" not in str(calls)


@pytest.mark.parametrize("failure", ["exit_status", "timeout", "could_not_start"])
@pytest.mark.parametrize("index", [0, 3, 6])
def test_failure_stops_and_is_not_reported_as_ready(root: Path, failure: str, index: int) -> None:
    calls = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[Any]:
        calls.append(command)
        if len(calls) - 1 == index:
            if failure == "timeout":
                raise subprocess.TimeoutExpired(command, 300)
            if failure == "could_not_start":
                raise FileNotFoundError
            return subprocess.CompletedProcess(command, 9)
        return subprocess.CompletedProcess(command, 0)

    report = quality.run_quality(root, run=run)
    assert report["status"] == "failed"
    assert len(calls) == index + 1
    assert report["checks"][-1]["reason"] == failure
    assert report["checks"][-1]["name"] == quality.CHECKS[index][0]


@pytest.mark.parametrize("timeout", [0, -1, 3601, True, 1.5])
def test_invalid_timeout_does_not_execute(root: Path, timeout: int) -> None:
    with pytest.raises(ValueError, match="timeout_seconds"):
        quality.run_quality(root, timeout_seconds=timeout,
                            run=lambda *_a, **_k: pytest.fail("must not run"))


def test_missing_repository_root_does_not_execute(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="root must contain"):
        quality.run_quality(tmp_path, run=lambda *_a, **_k: pytest.fail("must not run"))


@pytest.mark.parametrize("status", ["ready", "failed"])
def test_main_emits_one_json_result_and_matching_exit_status(
    root: Path, status: str, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def run(selected: Path, **kwargs: Any) -> dict[str, Any]:
        assert selected == root
        assert kwargs["timeout_seconds"] == 30
        return {"status": status, "checks": []}

    monkeypatch.setattr(quality, "run_quality", run)
    assert quality.main(["--root", str(root), "--timeout-seconds", "30"]) == (status != "ready")
    assert json.loads(capsys.readouterr().out)["status"] == status


def test_main_rejects_invalid_root(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as error:
        quality.main(["--root", str(tmp_path)])
    assert error.value.code == 2
