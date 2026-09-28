import json
import runpy
from pathlib import Path
from typing import Any

import httpx
import pytest

from lakehouse_ops import cli
from lakehouse_ops.commerce_gold_gate import check_commerce_gold_with_retry
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock


def test_fault_drill_delegates_recovery_and_retains_attempt_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    exercise = runpy.run_path("tests/integration/exercise_commerce_runner_retry.py")["exercise"]
    real_client = httpx.Client
    forwarded = []

    def upstream(request: httpx.Request) -> httpx.Response:
        forwarded.append(request)
        return httpx.Response(200, request=request, json={
            "id": "test-query",
            "columns": [{"name": name} for name in (
                "days", "orders", "captured_revenue_cents", "invalid_days",
            )],
            "data": [[1, 2, 100, 0]],
            "stats": {"state": "FINISHED", **dict.fromkeys((
                "elapsedTimeMillis", "wallTimeMillis", "cpuTimeMillis", "processedRows",
                "processedBytes", "physicalInputBytes", "peakMemoryBytes", "spilledBytes",
            ), 0)},
        })

    def client(**kwargs: Any) -> httpx.Client:
        kwargs.setdefault("transport", httpx.MockTransport(upstream))
        return real_client(**kwargs)

    def command(arguments: list[str]) -> int:
        assert arguments == ["run-commerce-batch", "--attempts", "3"]
        with commerce_runner_lock(), cli.TrinoClient("http://test.invalid", user="test") as trino:
            cli.run_compose_stage("bronze-input-sync", {}, bucket="test")
            report = check_commerce_gold_with_retry(
                trino.query, "a" * 16, attempts=3, delay_seconds=0,
            )
        assert report.orders == 2
        return 0

    monkeypatch.setattr(httpx, "Client", client)
    monkeypatch.setattr(cli, "main", command)
    monkeypatch.setattr(cli, "run_compose_stage", lambda *args, **kwargs: None)
    monkeypatch.chdir(tmp_path)
    evidence = tmp_path / "artifacts" / "retry.json"
    exercise(["--attempts", "3"], evidence)
    assert len(forwarded) == 1
    assert json.loads(evidence.read_text()) == {
        "status": "ready", "injected_http_status": 503, "statement_attempts": 2,
        "verification_backend": "real_trino",
        "competing_runner_exit_code": 2,
    }
