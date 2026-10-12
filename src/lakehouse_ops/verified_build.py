from __future__ import annotations

import argparse
import subprocess
import sys
import time
from collections.abc import Callable, Sequence

RunOnce = Callable[[Sequence[str]], tuple[int, str]]
Sleep = Callable[[float], None]


def is_retryable_failure(output: str) -> bool:
    return "digest mismatch" in output.casefold()


def run_once(command: Sequence[str]) -> tuple[int, str]:
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if process.stdout is None:
        raise RuntimeError("verified build could not capture process output")

    output: list[str] = []
    for line in process.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        output.append(line)
    return process.wait(), "".join(output)


def run_verified_build(
    command: Sequence[str],
    *,
    attempts: int = 3,
    delay_seconds: float = 5,
    run: RunOnce = run_once,
    sleep: Sleep = time.sleep,
    maven_central_fallback: bool = False,
) -> int:
    if not command:
        raise ValueError("verified build command is required")
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    if delay_seconds < 0:
        raise ValueError("delay_seconds must be non-negative")

    current = list(command)
    if maven_central_fallback and (
        current[:2] != ["docker", "compose"] or "build" not in current[2:]
        or any("MAVEN_CENTRAL_URL=" in argument for argument in current)
    ):
        raise ValueError("Maven fallback requires Compose build without an explicit endpoint")

    for attempt in range(1, attempts + 1):
        return_code, output = run(current)
        if return_code == 0:
            return 0
        if attempt == attempts or not is_retryable_failure(output):
            return return_code

        if maven_central_fallback and attempt == 1:
            position = current.index("build") + 1
            current[position:position] = [
                "--build-arg", "MAVEN_CENTRAL_URL=https://repo.maven.apache.org/maven2",
            ]
            print("Retrying through the alternate Maven Central endpoint; checksums unchanged.",
                  file=sys.stderr)

        print(
            f"Checksum mismatch detected; retrying verified build "
            f"({attempt + 1}/{attempts}).",
            file=sys.stderr,
        )
        sleep(delay_seconds)

    raise AssertionError("verified build retry loop exhausted unexpectedly")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Retry checksum-rejected external artifacts without bypassing verification."
    )
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--delay-seconds", type=float, default=5)
    parser.add_argument("--maven-central-fallback", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a build command is required after --")
    try:
        return run_verified_build(
            command,
            attempts=args.attempts,
            delay_seconds=args.delay_seconds,
            maven_central_fallback=args.maven_central_fallback,
        )
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
