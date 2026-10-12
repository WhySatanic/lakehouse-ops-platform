from __future__ import annotations

import json
import sqlite3
import subprocess
from contextlib import closing
from pathlib import Path

import httpx
import pytest
from integration import exercise_commerce_runner_retry as retry_exercise
from test_cli import FakeS3Client
from test_trino import final_stats

from lakehouse_ops import cli, trino
from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock


@pytest.mark.parametrize("setting", [None, "1", "86400"])
def test_runner_query_deadline_cancels_without_checkpoint_then_retries(
    setting: str | None, monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    if setting is None:
        monkeypatch.delenv("COMMERCE_QUERY_TIMEOUT_SECONDS", raising=False)
    else:
        monkeypatch.setenv("COMMERCE_QUERY_TIMEOUT_SECONDS", setting)
    budget = 120 if setting is None else int(setting)
    fixture = cli.generate_commerce_fixture(tmp_path / "fixture", cli.CommerceFixtureConfig(
        customers=4, products=2, orders=6, null_customer_emails=1,
        duplicate_orders=1, late_orders=1, invalid_payments=1,
    ))
    s3 = FakeS3Client()
    cli.CommerceS3LandingZone(s3, bucket="lakehouse").write(fixture.path)
    monkeypatch.setattr(cli, "_create_s3_client", lambda _: s3)
    state = tmp_path / "checkpoint.json"
    stages, methods = [], []
    monkeypatch.setattr(cli, "run_compose_stage", lambda service, *a, **k: stages.append(service))
    clock = [0.0]
    monkeypatch.setattr(trino, "monotonic", lambda: clock[0])
    fail = [True]

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        assert not state.exists()
        if request.method == "DELETE":
            assert str(request.url) == "http://localhost:8080/v1/statement/owned/2"
            return httpx.Response(204)
        if fail[0]:
            if request.method == "GET":
                clock[0] += budget + 1
            return httpx.Response(200, json={
                "id": "owned", "nextUri": "http://internal/v1/statement/owned/2",
            })
        return httpx.Response(200, json={
            "id": "done", "columns": [{"name": name} for name in
                ("days", "orders", "captured_revenue_cents", "invalid_days")],
            "data": [[2, 6, 1000, 0]],
            "stats": final_stats(),
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        def client(server: str, **kwargs: object) -> trino.TrinoClient:
            assert kwargs.get("query_timeout") == budget
            return trino.TrinoClient(server, client=http, **kwargs)

        monkeypatch.setattr(cli, "TrinoClient", client)
        args = ["run-commerce-batch", "--s3-bucket", "lakehouse", "--state", str(state),
                "--attempts", "3", "--retry-delay-seconds", "0"]
        with pytest.raises(SystemExit) as caught:
            cli.main(args)
        assert caught.value.code == 2
        assert "verify-commerce-gold" in capsys.readouterr().err
        assert not state.exists()
        assert methods == ["POST", "GET", "DELETE"]
        assert stages == list(COMMERCE_STAGES)
        with closing(sqlite3.connect(str(state) + ".runs.sqlite3")) as connection:
            assert connection.execute(
                "SELECT event FROM commerce_run_events ORDER BY sequence DESC LIMIT 1",
            ).fetchone()[0] == "failed"
        with commerce_runner_lock():
            pass
        fail[0] = False
        assert cli.main(args) == 0
        assert json.loads(capsys.readouterr().out)["checkpoint"]["created"] is True
        assert methods == ["POST", "GET", "DELETE", "POST"]
        assert stages == list(COMMERCE_STAGES) * 2


@pytest.mark.parametrize("value", ["", "0", "-1", "86401", "nan", "inf", "1.5"])
def test_runner_rejects_bad_query_budget_before_s3(
    value: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("COMMERCE_QUERY_TIMEOUT_SECONDS", value)
    monkeypatch.setattr(cli, "_create_s3_client", lambda _: pytest.fail("must not access S3"))
    with pytest.raises(SystemExit) as caught:
        cli.main(["run-commerce-batch", "--s3-bucket", "lakehouse"])
    assert caught.value.code == 2
    assert "COMMERCE_QUERY_TIMEOUT_SECONDS" in capsys.readouterr().err


def test_ci_retry_adapter_preserves_the_runner_query_budget(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("COMMERCE_QUERY_TIMEOUT_SECONDS", "7")
    fixture = cli.generate_commerce_fixture(tmp_path / "fixture", cli.CommerceFixtureConfig(
        customers=4, products=2, orders=6, null_customer_emails=1,
        duplicate_orders=1, late_orders=1, invalid_payments=1,
    ))
    s3 = FakeS3Client()
    cli.CommerceS3LandingZone(s3, bucket="lakehouse").write(fixture.path)
    monkeypatch.setattr(cli, "_create_s3_client", lambda _: s3)
    monkeypatch.setattr(cli, "run_compose_stage", lambda *a, **k: None)
    monkeypatch.setattr(retry_exercise.subprocess, "run", lambda command, **kwargs:
        subprocess.CompletedProcess(command, 2, "", "another commerce runner holds"))
    original_client = httpx.Client
    requests = []

    def healthy(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert 0 < request.extensions["timeout"]["read"] <= 7
        return httpx.Response(200, json={
            "id": "healthy", "columns": [{"name": name} for name in
                ("days", "orders", "captured_revenue_cents", "invalid_days")],
            "data": [[2, 6, 1000, 0]], "stats": final_stats(),
        })

    def client(**kwargs: object) -> httpx.Client:
        if "transport" not in kwargs:
            kwargs["transport"] = httpx.MockTransport(healthy)
        return original_client(**kwargs)

    monkeypatch.setattr(retry_exercise.httpx, "Client", client)
    evidence = tmp_path / "retry.json"
    retry_exercise.exercise([
        "--s3-bucket", "lakehouse", "--state", str(tmp_path / "checkpoint.json"),
        "--attempts", "2", "--retry-delay-seconds", "0",
    ], evidence)
    assert json.loads(evidence.read_text())["statement_attempts"] == 2
    assert len(requests) == 1
    assert (tmp_path / "checkpoint.json").exists()
