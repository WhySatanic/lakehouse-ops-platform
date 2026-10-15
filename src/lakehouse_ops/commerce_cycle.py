"""One scheduled commerce attempt followed by a freshness notification."""

from __future__ import annotations

import argparse
import math
import os
import subprocess
import sys
import time
from collections.abc import Sequence

from lakehouse_ops.commerce_alerts import CommerceAlertError, validate_commerce_alert_options
from lakehouse_ops.commerce_gold_gate import CommerceGoldGateError, validate_commerce_gold_retry
from lakehouse_ops.commerce_pipeline import (
    COMMERCE_CYCLE_DEADLINE_ENV,
    COMMERCE_RUN_CLEANUP_GRACE_SECONDS,
    CommercePipelineError,
    commerce_run_timeout_seconds,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument("--s3-endpoint-url", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--alertmanager-server", required=True)
    parser.add_argument("--source-max-age-seconds", type=int, default=900)
    parser.add_argument("--backlog-max-age-seconds", type=int, default=900)
    parser.add_argument("--alert-valid-seconds", type=int, default=180)
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--retry-delay-seconds", type=float, default=2)
    return parser


def run_cycle(args: argparse.Namespace) -> int:
    deadline = time.monotonic() + commerce_run_timeout_seconds()
    validate_commerce_gold_retry(args.attempts, args.retry_delay_seconds)
    validate_commerce_alert_options(
        server=args.alertmanager_server, instance=args.instance,
        source_max_age_seconds=args.source_max_age_seconds,
        backlog_max_age_seconds=args.backlog_max_age_seconds,
        valid_seconds=args.alert_valid_seconds,
    )
    shared = [
        "--s3-bucket", args.s3_bucket, "--s3-endpoint-url", args.s3_endpoint_url,
        "--state", args.state,
    ]
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        print("commerce cycle time budget exhausted before runner", file=sys.stderr)
        return 2
    runner_code = _run_command(
        [sys.executable, "-m", "lakehouse_ops.cli", "run-commerce-batch", *shared,
         "--server", args.server, "--user", args.user,
         "--attempts", str(args.attempts),
         "--retry-delay-seconds", str(args.retry_delay_seconds)],
        "run-commerce-batch",
        # The runner may spend up to 30 seconds removing its timed-out stage.
        # A small margin also permits its failed journal event and process exit.
        timeout=remaining + COMMERCE_RUN_CLEANUP_GRACE_SECONDS,
        environment={
            **os.environ, "COMMERCE_RUN_TIMEOUT_SECONDS": str(math.ceil(remaining)),
            COMMERCE_CYCLE_DEADLINE_ENV: repr(deadline),
        },
    )
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        print("commerce cycle time budget exhausted; notification skipped", file=sys.stderr)
        return runner_code or 2
    notification_code = _run_command(
        [sys.executable, "-m", "lakehouse_ops.cli", "notify-commerce-freshness", *shared,
         "--instance", args.instance, "--alertmanager-server", args.alertmanager_server,
         "--source-max-age-seconds", str(args.source_max_age_seconds),
         "--backlog-max-age-seconds", str(args.backlog_max_age_seconds),
         "--alert-valid-seconds", str(args.alert_valid_seconds)],
        "notify-commerce-freshness",
        timeout=remaining,
    )
    # A failed pipeline must not be hidden by a successful notification. A freshness
    # breach remains visible as exit 1 when the pipeline itself succeeded or was idle.
    return runner_code or notification_code


def _run_command(
    command: list[str], name: str, *, timeout: float,
    environment: dict[str, str] | None = None,
) -> int:
    try:
        return subprocess.run(command, check=False, timeout=timeout, env=environment).returncode
    except subprocess.TimeoutExpired:
        print(f"{name} timed out after {timeout:.3f}s", file=sys.stderr)
        if name == "run-commerce-batch":
            print("runner terminated; container cleanup unconfirmed; inspect its journal "
                  "before retrying", file=sys.stderr)
        return 2
    except OSError as error:
        print(f"{name} could not start: {error}", file=sys.stderr)
        return 2


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run_cycle(args)
    except (CommerceAlertError, CommerceGoldGateError, CommercePipelineError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
