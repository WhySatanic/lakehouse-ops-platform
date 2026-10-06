from __future__ import annotations

import subprocess
import sys
from typing import Any

import pytest

from lakehouse_ops import commerce_cycle


@pytest.mark.parametrize(
    ("runner_code", "notification_code", "expected"),
    [(0, 0, 0), (0, 1, 1), (0, 2, 2), (2, 0, 2), (2, 1, 2), (2, 2, 2)],
)
def test_scheduled_cycle_notifies_even_after_pipeline_failure(
    monkeypatch: pytest.MonkeyPatch,
    runner_code: int,
    notification_code: int,
    expected: int,
) -> None:
    calls: list[list[str]] = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert kwargs == {"check": False}
        calls.append(command)
        return subprocess.CompletedProcess(
            command, [runner_code, notification_code][len(calls) - 1],
        )

    monkeypatch.setattr(commerce_cycle.subprocess, "run", run)
    result = commerce_cycle.main([
        "--s3-bucket", "lakehouse", "--s3-endpoint-url", "http://localhost:9000",
        "--state", "data/state/commerce-batches.json",
        "--server", "http://localhost:8080", "--user", "lakehouse-ops",
        "--instance", "commerce-local", "--alertmanager-server", "http://localhost:9093",
        "--attempts", "3", "--retry-delay-seconds", "0",
        "--source-max-age-seconds", "600", "--backlog-max-age-seconds", "300",
        "--alert-valid-seconds", "180",
    ])
    assert result == expected
    assert len(calls) == 2
    assert calls[0][:4] == [sys.executable, "-m", "lakehouse_ops.cli", "run-commerce-batch"]
    assert calls[1][:4] == [sys.executable, "-m", "lakehouse_ops.cli", "notify-commerce-freshness"]
    for command in calls:
        assert command[command.index("--s3-bucket") + 1] == "lakehouse"
        assert command[command.index("--state") + 1] == "data/state/commerce-batches.json"
    assert calls[0][calls[0].index("--attempts") + 1] == "3"
    assert calls[1][calls[1].index("--backlog-max-age-seconds") + 1] == "300"


@pytest.mark.parametrize("failed_command", ["run-commerce-batch", "notify-commerce-freshness"])
def test_cycle_reports_spawn_failure_and_attempts_notification(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failed_command: str,
) -> None:
    calls: list[str] = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert kwargs == {"check": False}
        calls.append(command[3])
        if command[3] == failed_command:
            raise OSError("process limit reached")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(commerce_cycle.subprocess, "run", run)

    result = commerce_cycle.main([
        "--s3-bucket", "lakehouse", "--s3-endpoint-url", "http://localhost:9000",
        "--state", "data/state/commerce-batches.json",
        "--server", "http://localhost:8080", "--user", "lakehouse-ops",
        "--instance", "commerce-local", "--alertmanager-server", "http://localhost:9093",
    ])

    assert result == 2
    assert calls == ["run-commerce-batch", "notify-commerce-freshness"]
    assert f"{failed_command} could not start: process limit reached" in capsys.readouterr().err


def test_cycle_requires_scope_before_starting_work(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        commerce_cycle.subprocess, "run", lambda *_args, **_kwargs: pytest.fail("must not run"),
    )
    with pytest.raises(SystemExit) as error:
        commerce_cycle.main(["--s3-bucket", "lakehouse"])
    assert error.value.code == 2


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--alertmanager-server", "ftp://alertmanager"),
        ("--instance", "invalid instance"),
        ("--alert-valid-seconds", "29"),
        ("--source-max-age-seconds", "0"),
        ("--backlog-max-age-seconds", "0"),
        ("--attempts", "0"),
        ("--retry-delay-seconds", "-1"),
    ],
)
def test_invalid_cycle_configuration_fails_before_starting_pipeline(
    monkeypatch: pytest.MonkeyPatch, option: str, value: str,
) -> None:
    monkeypatch.setattr(
        commerce_cycle.subprocess, "run", lambda *_args, **_kwargs: pytest.fail("must not run"),
    )
    with pytest.raises(SystemExit) as error:
        commerce_cycle.main([
            "--s3-bucket", "lakehouse", "--s3-endpoint-url", "http://localhost:9000",
            "--state", "data/state/commerce-batches.json",
            "--server", "http://localhost:8080", "--user", "lakehouse-ops",
            "--instance", "commerce-local", "--alertmanager-server", "http://localhost:9093",
            option, value,
        ])
    assert error.value.code == 2
