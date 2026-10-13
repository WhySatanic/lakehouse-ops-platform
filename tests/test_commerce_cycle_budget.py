from __future__ import annotations

import subprocess
import sys
import time

import pytest

from lakehouse_ops import commerce_cycle as cycle

OPTIONS = [
    "--s3-bucket", "lakehouse", "--s3-endpoint-url", "http://localhost:9000",
    "--state", "unused.json", "--server", "http://localhost:8080", "--user", "lakehouse-ops",
    "--instance", "commerce-local", "--alertmanager-server", "http://localhost:9093",
]


@pytest.mark.parametrize("elapsed", [4, 10])
def test_notification_uses_only_unspent_cycle_budget(monkeypatch, elapsed, capsys):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", "10")
    now = 0
    calls = []

    def run(command, **kwargs):
        nonlocal now
        calls.append(kwargs)
        if len(calls) == 1:
            now = elapsed
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cycle.time, "monotonic", lambda: now)
    monkeypatch.setattr(cycle.subprocess, "run", run)
    assert cycle.main(OPTIONS) == (0 if elapsed < 10 else 2)
    assert calls[0]["timeout"] == 45
    assert calls[0]["env"]["COMMERCE_RUN_TIMEOUT_SECONDS"] == "10"
    if elapsed < 10:
        assert calls[1]["timeout"] == 6
    else:
        assert len(calls) == 1
        assert "notification skipped" in capsys.readouterr().err


@pytest.mark.parametrize("value", ["0", "86401", "bad"])
def test_invalid_cycle_budget_starts_no_children(monkeypatch, value):
    monkeypatch.setenv("COMMERCE_RUN_TIMEOUT_SECONDS", value)
    monkeypatch.setattr(cycle.subprocess, "run", lambda *a, **kw: pytest.fail("no child"))
    with pytest.raises(SystemExit) as error:
        cycle.main(OPTIONS)
    assert error.value.code == 2


def test_timeout_is_failure_and_does_not_claim_container_cleanup(monkeypatch, capsys):
    def run(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    monkeypatch.setattr(cycle.subprocess, "run", run)
    assert cycle._run_command(["unused"], "run-commerce-batch", timeout=1) == 2
    assert "container cleanup unconfirmed" in capsys.readouterr().err


def test_real_sleeping_child_is_bounded(capsys):
    started = time.monotonic()
    code = cycle._run_command(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        "notify-commerce-freshness", timeout=0.2,
    )
    assert code == 2
    assert time.monotonic() - started < 5
    assert "timed out" in capsys.readouterr().err
