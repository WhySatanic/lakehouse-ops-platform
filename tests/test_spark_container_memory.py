from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest


@pytest.fixture
def metrics(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "jobs" / "spark"))
    return importlib.import_module("container_memory")


@pytest.mark.parametrize("peak,current", [(123456, 4096), (0, 0)])
def test_private_container_measurement(metrics, tmp_path: Path, peak: int, current: int):
    (tmp_path / "membership").write_text("0::/\n")
    (tmp_path / "memory.peak").write_text(str(peak))
    (tmp_path / "memory.current").write_text(str(current))
    assert metrics.memory_observation(tmp_path, tmp_path / "membership") == {
        "status": "observed", "peak_bytes": peak, "current_bytes": current,
    }


@pytest.mark.parametrize("membership,peak,current", [
    ("0::/host.slice", "100", "50"), ("1:memory:/", "100", "50"),
    ("0::/", "max", "50"), ("0::/", "-1", "50"), ("0::/", "100", "-1"),
])
def test_unsupported_or_invalid_is_not_a_zero_measurement(
    metrics, tmp_path: Path, membership: str, peak: str, current: str,
):
    (tmp_path / "membership").write_text(membership)
    (tmp_path / "memory.peak").write_text(peak)
    (tmp_path / "memory.current").write_text(current)
    assert metrics.memory_observation(tmp_path, tmp_path / "membership") == {
        "status": "unavailable", "peak_bytes": None, "current_bytes": None,
    }


def test_missing_controller_is_unavailable(metrics, tmp_path: Path):
    assert metrics.memory_observation(tmp_path, tmp_path / "missing")["status"] == "unavailable"


@pytest.mark.parametrize("enabled", [False, True])
def test_opt_in_exit_report_keeps_stdout_clean(
    metrics, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, enabled: bool,
):
    callbacks = []
    monkeypatch.setenv("LAKEOPS_SPARK_MEMORY_REPORT", "1" if enabled else "0")
    monkeypatch.setenv("COMMERCE_BATCH_ID", "test-batch")
    monkeypatch.setattr(metrics.atexit, "register", callbacks.append)
    monkeypatch.setattr(metrics, "memory_observation", lambda: {
        "status": "observed", "peak_bytes": 123456, "current_bytes": 4096,
    })
    metrics.register_memory_report("commerce-test")
    assert len(callbacks) == int(enabled)
    if enabled:
        callbacks[0]()
    captured = capsys.readouterr()
    assert captured.out == ""
    if enabled:
        report = json.loads(captured.err)
        assert report["app_name"] == "commerce-test"
        assert report["batch_id"] == "test-batch"
        assert report["peak_bytes"] == 123456
        assert report["elapsed_seconds"] >= 0
    else:
        assert captured.err == ""
