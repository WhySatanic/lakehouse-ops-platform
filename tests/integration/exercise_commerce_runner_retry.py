from __future__ import annotations

import argparse
import json
import subprocess
import sys
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import httpx

from lakehouse_ops import cli
from lakehouse_ops.trino import TrinoClient


def exercise(arguments: list[str], evidence: Path) -> None:
    statements = 0
    rejected = 0
    run_stage = cli.run_compose_stage

    def stage(service: str, batch: dict[str, str], *, bucket: str) -> None:
        nonlocal rejected
        if rejected == 0:
            competitor = subprocess.run(
                [sys.executable, "-c",
                 "from lakehouse_ops.cli import main; raise SystemExit(main())",
                 "run-commerce-batch", "--s3-bucket", bucket,
                 "--state", "data/state/commerce-competing-ci.json"],
                capture_output=True, text=True, timeout=15, check=False,
            )
            assert competitor.returncode == 2, competitor.stderr
            assert "another commerce runner holds" in competitor.stderr
            assert competitor.stdout == ""
            rejected += 1
        run_stage(service, batch, bucket=bucket)

    with ExitStack() as stack:
        upstream = stack.enter_context(httpx.Client(timeout=30))

        def forward(request: httpx.Request) -> httpx.Response:
            nonlocal statements
            if request.method == "POST":
                assert request.content.decode().startswith("SELECT count(*) AS days")
                statements += 1
                if statements == 1:
                    return httpx.Response(503, request=request, text="injected coordinator outage")
            return upstream.send(request)

        transport = httpx.MockTransport(forward)
        client = stack.enter_context(httpx.Client(transport=transport))

        def trino(server: str, *, user: str) -> TrinoClient:
            return TrinoClient(server, user=user, client=client)

        with patch.object(cli, "TrinoClient", trino), patch.object(cli, "run_compose_stage", stage):
            assert cli.main(["run-commerce-batch", *arguments]) == 0
        assert statements == 2, "expected one rejected statement and one real Trino verification"
        assert rejected == 1, "expected competitor rejection while the pipeline held its lock"
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(json.dumps({
        "status": "ready", "injected_http_status": 503, "statement_attempts": statements,
        "verification_backend": "real_trino",
        "competing_runner_exit_code": 2,
    }, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Inject one HTTP 503 before real gold verification"
    )
    parser.add_argument("--retry-evidence", type=Path, required=True)
    options, command_arguments = parser.parse_known_args()
    exercise(command_arguments, options.retry_evidence)
