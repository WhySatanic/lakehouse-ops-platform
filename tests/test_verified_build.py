from __future__ import annotations

from collections.abc import Sequence

import pytest

from lakehouse_ops.verified_build import run_verified_build


class FakeRunner:
    def __init__(self, results: list[tuple[int, str]]) -> None:
        self.results = iter(results)
        self.commands: list[list[str]] = []

    def __call__(self, command: Sequence[str]) -> tuple[int, str]:
        self.commands.append(list(command))
        return next(self.results)


def test_verified_build_returns_after_first_success() -> None:
    runner = FakeRunner([(0, "built")])
    delays: list[float] = []

    result = run_verified_build(["docker", "build"], run=runner, sleep=delays.append)

    assert result == 0
    assert runner.commands == [["docker", "build"]]
    assert delays == []


def test_verified_build_retries_checksum_mismatch() -> None:
    runner = FakeRunner([(1, "ERROR: digest mismatch"), (0, "built")])
    delays: list[float] = []

    result = run_verified_build(
        ["docker", "build"], delay_seconds=2, run=runner, sleep=delays.append
    )

    assert result == 0
    assert runner.commands == [["docker", "build"], ["docker", "build"]]
    assert delays == [2]


def test_verified_build_does_not_retry_deterministic_failure() -> None:
    runner = FakeRunner([(17, "Dockerfile syntax error")])
    delays: list[float] = []

    result = run_verified_build(["docker", "build"], run=runner, sleep=delays.append)

    assert result == 17
    assert len(runner.commands) == 1
    assert delays == []


def test_verified_build_stops_after_bounded_attempts() -> None:
    runner = FakeRunner([(9, "digest mismatch")] * 3)
    delays: list[float] = []

    result = run_verified_build(
        ["docker", "build"], attempts=3, run=runner, sleep=delays.append
    )

    assert result == 9
    assert len(runner.commands) == 3
    assert delays == [5, 5]


@pytest.mark.parametrize(
    ("command", "attempts", "delay", "message"),
    [
        ([], 3, 5, "command"),
        (["docker"], 0, 5, "attempts"),
        (["docker"], 3, -1, "delay_seconds"),
    ],
)
def test_verified_build_rejects_invalid_configuration(
    command: list[str], attempts: int, delay: float, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        run_verified_build(command, attempts=attempts, delay_seconds=delay)
