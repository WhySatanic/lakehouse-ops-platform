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


def test_opt_in_compose_retry_switches_endpoint_but_not_checksum():
    command = ["docker", "compose", "--profile", "compute", "build", "spark-bronze"]
    runner = FakeRunner([(1, "ERROR: digest mismatch"), (1, "digest mismatch"), (0, "built")])
    assert run_verified_build(command, run=runner, sleep=lambda _: None,
                              maven_central_fallback=True) == 0
    fallback = [*command[:5], "--build-arg",
                "MAVEN_CENTRAL_URL=https://repo.maven.apache.org/maven2", *command[5:]]
    assert runner.commands == [command, fallback, fallback]
    assert command[-1] == "spark-bronze"


@pytest.mark.parametrize("command", [["docker", "build"], ["docker", "compose", "up"],
                                    ["docker", "compose", "build", "--build-arg",
                                     "MAVEN_CENTRAL_URL=https://custom.invalid", "spark-bronze"]])
def test_fallback_rejects_non_compose_build_or_explicit_endpoint(command):
    with pytest.raises(ValueError):
        run_verified_build(command, maven_central_fallback=True)


@pytest.mark.parametrize("result", [(0, "built"), (17, "Dockerfile syntax error")])
def test_opt_in_does_not_switch_or_retry_without_rejected_checksum(result):
    command = ["docker", "compose", "build", "spark-bronze"]
    runner = FakeRunner([result])
    delays = []
    assert run_verified_build(command, run=runner, sleep=delays.append,
                              maven_central_fallback=True) == result[0]
    assert runner.commands == [command]
    assert delays == []


def test_cli_forwards_opt_in_and_preserves_command(monkeypatch):
    from lakehouse_ops import verified_build

    captured = {}

    def build(command, **options):
        captured.update(command=command, **options)
        return 9

    monkeypatch.setattr(verified_build, "run_verified_build", build)
    assert verified_build.main(["--maven-central-fallback", "--attempts", "2",
                                "--delay-seconds", "0", "--", "docker", "compose",
                                "build", "spark-bronze"]) == 9
    assert captured == {"command": ["docker", "compose", "build", "spark-bronze"],
                        "attempts": 2, "delay_seconds": 0, "maven_central_fallback": True}
