from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import httpx
import pytest

from lakehouse_ops import __version__, cli
from lakehouse_ops.ingestion.models import Location, WeatherPayload


class FakeOpenMeteoClient:
    payload: dict[str, Any]

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def fetch(self, location: Location, *, forecast_days: int) -> WeatherPayload:
        assert forecast_days == 2
        return WeatherPayload.from_source(location, self.payload)


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.metadata: dict[tuple[str, str], dict[str, str]] = {}
        self.last_modified: dict[tuple[str, str], datetime] = {}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        object_id = (kwargs["Bucket"], kwargs["Key"])
        self.objects[object_id] = kwargs["Body"]
        self.metadata[object_id] = kwargs["Metadata"]
        self.last_modified[object_id] = datetime.now(UTC)
        return {"ETag": '"test"'}

    def get_bucket_location(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["Bucket"] == "lakehouse"
        return {}

    def get_bucket_versioning(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["Bucket"] == "lakehouse"
        return {"Status": "Enabled"}

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        prefix = kwargs.get("Prefix", "")
        return {
            "Contents": [
                {"Key": key, "LastModified": self.last_modified[(bucket, key)]}
                for bucket, key in sorted(self.objects)
                if bucket == kwargs["Bucket"] and key.startswith(prefix)
            ],
            "IsTruncated": False,
        }

    def list_object_versions(self, **kwargs: Any) -> dict[str, Any]:
        prefix = kwargs.get("Prefix", "")
        return {
            "Versions": [
                {"Key": key, "VersionId": "v1", "IsLatest": True}
                for bucket, key in sorted(self.objects)
                if bucket == kwargs["Bucket"] and key.startswith(prefix)
            ],
            "IsTruncated": False,
        }

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        object_id = (kwargs["Bucket"], kwargs["Key"])
        return {
            "Body": BytesIO(self.objects[object_id]),
            "Metadata": self.metadata[object_id],
        }


def test_version_command_reports_package_version(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as error:
        cli.main(["--version"])

    assert error.value.code == 0
    assert capsys.readouterr().out == f"lakeops {__version__}\n"


def test_generate_commerce_fixture_command(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    exit_code = cli.main(
        [
            "generate-commerce-fixture",
            "--output",
            str(tmp_path),
            "--customers",
            "4",
            "--products",
            "2",
            "--orders",
            "6",
            "--null-customer-emails",
            "1",
            "--duplicate-orders",
            "2",
            "--late-orders",
            "1",
            "--invalid-payments",
            "1",
        ]
    )

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert report["created"] is True
    assert report["tables"] == {
        "customers": 4,
        "orders": 8,
        "payments": 6,
        "products": 2,
    }
    assert Path(report["path"], "manifest.json").is_file()


def test_generate_commerce_fixture_rejects_corrupt_cached_manifest_at_cli(
    capsys: pytest.CaptureFixture[str], tmp_path: Path,
) -> None:
    args = [
        "generate-commerce-fixture", "--output", str(tmp_path),
        "--customers", "2", "--products", "2", "--orders", "3",
        "--null-customer-emails", "1", "--duplicate-orders", "1",
        "--late-orders", "1", "--invalid-payments", "1",
    ]
    assert cli.main(args) == 0
    report = json.loads(capsys.readouterr().out)
    manifest_path = Path(report["path"]) / "manifest.json"
    manifest_path.write_text("[]", encoding="utf-8")

    with pytest.raises(SystemExit) as error:
        cli.main(args)
    captured = capsys.readouterr()
    assert error.value.code == 2
    assert captured.out == ""
    assert "manifest structure is invalid" in captured.err
    assert manifest_path.read_text(encoding="utf-8") == "[]"


def test_land_commerce_fixture_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    fixture_report = cli.generate_commerce_fixture(
        tmp_path,
        cli.CommerceFixtureConfig(
            customers=4,
            products=2,
            orders=6,
            null_customer_emails=1,
            duplicate_orders=1,
            late_orders=1,
            invalid_payments=1,
        ),
    )
    s3_client = FakeS3Client()
    monkeypatch.setattr(cli, "_create_s3_client", lambda args: s3_client)

    exit_code = cli.main(
        [
            "land-commerce-fixture",
            "--fixture",
            str(fixture_report.path),
            "--s3-bucket",
            "lakehouse",
        ]
    )

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert report["created"] == 5
    assert report["objects"] == 5
    assert report["path"].startswith("s3://lakehouse/landing/source=commerce/")


def test_land_commerce_fixture_rejects_invalid_manifest_without_upload(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    fixture_report = cli.generate_commerce_fixture(
        tmp_path,
        cli.CommerceFixtureConfig(
            customers=4,
            products=2,
            orders=6,
            null_customer_emails=1,
            duplicate_orders=1,
            late_orders=1,
            invalid_payments=1,
        ),
    )
    (fixture_report.path / "manifest.json").write_text("[]", encoding="utf-8")
    s3_client = FakeS3Client()
    monkeypatch.setattr(cli, "_create_s3_client", lambda args: s3_client)

    with pytest.raises(SystemExit) as error:
        cli.main(
            [
                "land-commerce-fixture",
                "--fixture",
                str(fixture_report.path),
                "--s3-bucket",
                "lakehouse",
            ]
        )

    captured = capsys.readouterr()
    assert error.value.code == 2
    assert captured.out == ""
    assert "fixture manifest is invalid" in captured.err
    assert s3_client.objects == {}


def test_plan_and_commit_commerce_batch_commands(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    fixture = cli.generate_commerce_fixture(
        tmp_path / "fixture",
        cli.CommerceFixtureConfig(
            customers=4,
            products=2,
            orders=6,
            null_customer_emails=1,
            duplicate_orders=1,
            late_orders=1,
            invalid_payments=1,
        ),
    )
    s3_client = FakeS3Client()
    cli.CommerceS3LandingZone(s3_client, bucket="lakehouse").write(fixture.path)
    monkeypatch.setattr(cli, "_create_s3_client", lambda args: s3_client)
    state = tmp_path / "state.json"

    plan_code = cli.main(
        [
            "plan-commerce-batches",
            "--s3-bucket",
            "lakehouse",
            "--state",
            str(state),
        ]
    )
    plan = json.loads(capsys.readouterr().out)
    backlog_args = [
        "check-commerce-backlog", "--s3-bucket", "lakehouse", "--state", str(state)
    ]
    assert cli.main(backlog_args) == 0
    assert json.loads(capsys.readouterr().out)["pending_batches"] == 1
    manifest = next(key for key in s3_client.last_modified if key[1].endswith("manifest.json"))
    s3_client.last_modified[manifest] = datetime.now(UTC) - timedelta(hours=1)
    assert cli.main(backlog_args) == 1
    assert json.loads(capsys.readouterr().out)["reason"] == "pending_age_limit_exceeded"
    commit_code = cli.main(
        [
            "commit-commerce-batch",
            "--s3-bucket",
            "lakehouse",
            "--state",
            str(state),
            "--batch-id",
            plan["batches"][0]["batch_id"],
        ]
    )
    commit = json.loads(capsys.readouterr().out)

    assert plan_code == 0
    assert plan["selected_batches"] == 1
    assert commit_code == 0
    assert commit["created"] is True
    assert state.is_file()
    assert cli.main(backlog_args) == 0
    assert json.loads(capsys.readouterr().out)["pending_batches"] == 0


def test_check_commerce_source_freshness_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    fixture = cli.generate_commerce_fixture(
        tmp_path / "fixture",
        cli.CommerceFixtureConfig(
            customers=4,
            products=2,
            orders=6,
            null_customer_emails=1,
            duplicate_orders=1,
            late_orders=1,
            invalid_payments=1,
        ),
    )
    s3_client = FakeS3Client()
    cli.CommerceS3LandingZone(s3_client, bucket="lakehouse").write(fixture.path)
    monkeypatch.setattr(cli, "_create_s3_client", lambda args: s3_client)
    args = ["check-commerce-source-freshness", "--s3-bucket", "lakehouse"]

    assert cli.main(args) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ready"
    manifest = next(key for key in s3_client.last_modified if key[1].endswith("manifest.json"))
    s3_client.last_modified[manifest] = datetime.now(UTC) - timedelta(hours=1)
    assert cli.main(args) == 1
    assert json.loads(capsys.readouterr().out)["reason"] == "age_limit_exceeded"


def test_notify_commerce_freshness_reads_landing_without_advancing_state(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    from lakehouse_ops import commerce_alerts

    fixture = cli.generate_commerce_fixture(tmp_path / "fixture", cli.CommerceFixtureConfig(
        customers=4, products=2, orders=6, null_customer_emails=1,
        duplicate_orders=1, late_orders=1, invalid_payments=1,
    ))
    s3_client = FakeS3Client()
    cli.CommerceS3LandingZone(s3_client, bucket="lakehouse").write(fixture.path)
    manifest = next(key for key in s3_client.last_modified if key[1].endswith("manifest.json"))
    s3_client.last_modified[manifest] = datetime.now(UTC) - timedelta(hours=1)
    monkeypatch.setattr(cli, "_create_s3_client", lambda args: s3_client)
    payloads = []

    def accept(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        return httpx.Response(200)

    original = httpx.Client
    monkeypatch.setattr(commerce_alerts.httpx, "Client", lambda **kwargs: original(
        transport=httpx.MockTransport(accept), **kwargs,
    ))
    state = tmp_path / "checkpoint.json"
    args = ["notify-commerce-freshness", "--s3-bucket", "lakehouse",
            "--instance", "commerce-test", "--state", str(state)]
    assert cli.main(args) == 1
    assert json.loads(capsys.readouterr().out)["notification"] == "accepted"
    assert not state.exists()

    planner = cli.CommerceBatchPlanner(s3_client, bucket="lakehouse", state_path=state)
    planner.commit(fixture.batch_id)
    before = state.read_bytes()
    s3_client.last_modified[manifest] = datetime.now(UTC)
    assert cli.main(args) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ready"
    assert [a["labels"] for a in payloads[0]] == [a["labels"] for a in payloads[1]]
    assert state.read_bytes() == before

    state.write_text("invalid json", encoding="utf-8")
    with pytest.raises(SystemExit) as error:
        cli.main(args)
    assert error.value.code == 2
    assert "checkpoint" in capsys.readouterr().err
    assert len(payloads) == 2


def test_check_commerce_gold_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class FakeTrinoClient:
        def __init__(self, server: str, *, user: str) -> None:
            assert server == "http://localhost:8080"
            assert user == "lakehouse-ops"

        def __enter__(self) -> FakeTrinoClient:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def query(self, sql: str) -> list[dict[str, int]]:
            assert "source_batch_id = 'aaaaaaaaaaaaaaaa'" in sql
            return [
                {"days": 2, "orders": 6, "captured_revenue_cents": 900, "invalid_days": 0}
            ]

    monkeypatch.setattr(cli, "TrinoClient", FakeTrinoClient)

    assert cli.main(["check-commerce-gold", "--batch-id", "a" * 16]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ready"


@pytest.mark.parametrize("result", ["ready", "stage_failure", "empty", "retry", "transport"])
def test_run_commerce_batch_command_preserves_state_on_failure_and_skips_idle(
    result: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES, CommercePipelineError
    from lakehouse_ops.commerce_runner_lock import CommerceRunnerBusyError, commerce_runner_lock

    monkeypatch.chdir(tmp_path)

    fixture = cli.generate_commerce_fixture(tmp_path / "fixture", cli.CommerceFixtureConfig(
        customers=4, products=2, orders=6, null_customer_emails=1,
        duplicate_orders=1, late_orders=1, invalid_payments=1,
    ))
    s3_client = FakeS3Client()
    cli.CommerceS3LandingZone(s3_client, bucket="lakehouse").write(fixture.path)
    state = tmp_path / "checkpoint.json"
    stages = []
    statements = []

    def create_s3(args: object) -> FakeS3Client:
        with pytest.raises(CommerceRunnerBusyError), commerce_runner_lock():
            pytest.fail("runner lock must cover S3 access")
        return s3_client

    original_commit = cli.CommerceBatchPlanner.commit

    def commit(planner: Any, batch_id: str, **kwargs: Any) -> dict[str, Any]:
        with pytest.raises(CommerceRunnerBusyError), commerce_runner_lock():
            pytest.fail("runner lock must cover checkpoint completion")
        return original_commit(planner, batch_id, **kwargs)

    monkeypatch.setattr(cli, "_create_s3_client", create_s3)
    monkeypatch.setattr(cli.CommerceBatchPlanner, "commit", commit)

    def execute(service: str, batch: dict[str, str], *, bucket: str) -> None:
        with pytest.raises(CommerceRunnerBusyError), commerce_runner_lock():
            pytest.fail("runner lock must cover Compose execution")
        assert bucket == "lakehouse"
        assert batch["batch_id"] == fixture.batch_id
        assert not state.exists()
        stages.append(service)
        if result == "stage_failure":
            raise CommercePipelineError(f"commerce stage failed: {service}")

    class FakeTrinoClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def __enter__(self) -> FakeTrinoClient:
            return self

        def __exit__(self, *_: object) -> None:
            pass

        def query(self, sql: str) -> list[dict[str, int]]:
            with pytest.raises(CommerceRunnerBusyError), commerce_runner_lock():
                pytest.fail("runner lock must cover Trino verification")
            assert len(stages) == len(COMMERCE_STAGES)
            assert not state.exists()
            statements.append(sql)
            if result == "transport" or (result == "retry" and len(statements) == 1):
                raise httpx.ConnectError("coordinator offline")
            return [{"days": 2 if result in {"ready", "retry"} else 0,
                     "orders": 6, "captured_revenue_cents": 1000, "invalid_days": 0}]

    monkeypatch.setattr(cli, "run_compose_stage", execute)
    monkeypatch.setattr(cli, "TrinoClient", FakeTrinoClient)
    args = ["run-commerce-batch", "--s3-bucket", "lakehouse", "--state", str(state),
            "--attempts", "3", "--retry-delay-seconds", "0"]
    if result in {"ready", "retry"}:
        assert cli.main(args) == 0
        report = json.loads(capsys.readouterr().out)
        assert report["checkpoint"]["created"] is True
        assert set(report["durations_seconds"]["stages"]) == set(COMMERCE_STAGES)
        before = state.read_bytes()
        assert cli.main(args) == 0
        assert json.loads(capsys.readouterr().out)["status"] == "idle"
        assert state.read_bytes() == before
        assert stages == list(COMMERCE_STAGES)
        assert len(statements) == (2 if result == "retry" else 1)
    else:
        with pytest.raises(SystemExit) as error:
            cli.main(args)
        assert error.value.code == 2
        assert "stage failed" in capsys.readouterr().err
        assert not state.exists()
        assert len(statements) == (3 if result == "transport" else int(result == "empty"))
    with pytest.raises(SystemExit) as error:
        cli.main([*args, "--s3-prefix", "other"])
    assert error.value.code == 2
    assert "requires --s3-prefix landing" in capsys.readouterr().err


@pytest.mark.parametrize("checkpoint", ["one.json", "another.json"])
def test_run_commerce_workspace_lock_rejects_competitor_before_s3(
    checkpoint: str, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], tmp_path: Path,
) -> None:
    from lakehouse_ops.commerce_runner_lock import commerce_runner_lock

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "_create_s3_client", lambda args: pytest.fail("must not access S3"))
    with commerce_runner_lock(), pytest.raises(SystemExit) as error:
        cli.main(["run-commerce-batch", "--s3-bucket", "lakehouse", "--state", checkpoint])
    assert error.value.code == 2
    captured = capsys.readouterr()
    assert "another commerce runner holds" in captured.err
    assert captured.out == ""
    assert not (tmp_path / checkpoint).exists()


def test_run_commerce_batch_rejects_non_object_checkpoint_before_compute(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    state = tmp_path / "checkpoint.json"
    state.write_text("[]", encoding="utf-8")
    before = state.read_bytes()
    monkeypatch.setattr(cli, "_create_s3_client", lambda args: FakeS3Client())
    monkeypatch.setattr(cli, "run_compose_stage", lambda *args, **kwargs: pytest.fail(
        "invalid checkpoint must not start Compose",
    ))
    monkeypatch.setattr(cli.TrinoClient, "query", lambda *args: pytest.fail(
        "invalid checkpoint must not query Trino",
    ))

    with pytest.raises(SystemExit) as error:
        cli.main(["run-commerce-batch", "--s3-bucket", "lakehouse", "--state", str(state)])
    captured = capsys.readouterr()
    assert error.value.code == 2
    assert "commerce checkpoint has an unsupported structure" in captured.err
    assert captured.out == ""
    assert state.read_bytes() == before


def test_run_commerce_releases_lock_after_s3_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path,
) -> None:
    from lakehouse_ops.commerce_runner_lock import commerce_runner_lock

    monkeypatch.chdir(tmp_path)

    def fail(args: object) -> None:
        raise OSError("S3 unavailable")

    monkeypatch.setattr(cli, "_create_s3_client", fail)
    with pytest.raises(SystemExit) as error:
        cli.main(["run-commerce-batch", "--s3-bucket", "lakehouse"])
    assert error.value.code == 2
    assert "S3 unavailable" in capsys.readouterr().err
    with commerce_runner_lock():
        pass


@pytest.mark.parametrize(
    ("option", "value"),
    [("--attempts", "0"), ("--attempts", "6"), ("--retry-delay-seconds", "-1"),
     ("--retry-delay-seconds", "61"), ("--retry-delay-seconds", "nan")],
)
def test_run_commerce_rejects_retry_bounds_before_s3(
    option: str, value: str, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli, "_create_s3_client", lambda args: pytest.fail("must not access S3"))
    with pytest.raises(SystemExit) as error:
        cli.main(["run-commerce-batch", "--s3-bucket", "lakehouse", option, value])
    assert error.value.code == 2
    captured = capsys.readouterr()
    assert "must be" in captured.err
    assert captured.out == ""


@pytest.mark.parametrize("result", ["ready", "empty", "transport", "retry"])
def test_complete_commerce_batch_only_checkpoints_verified_gold(
    result: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    fixture = cli.generate_commerce_fixture(
        tmp_path / "fixture",
        cli.CommerceFixtureConfig(
            customers=4, products=2, orders=6, null_customer_emails=1,
            duplicate_orders=1, late_orders=1, invalid_payments=1,
        ),
    )
    s3_client = FakeS3Client()
    cli.CommerceS3LandingZone(s3_client, bucket="lakehouse").write(fixture.path)
    monkeypatch.setattr(cli, "_create_s3_client", lambda args: s3_client)
    state = tmp_path / "checkpoint.json"
    calls = 0

    class FakeTrinoClient:
        def __init__(self, server: str, *, user: str) -> None:
            pass

        def __enter__(self) -> FakeTrinoClient:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def query(self, sql: str) -> list[dict[str, int]]:
            nonlocal calls
            calls += 1
            assert fixture.batch_id in sql
            if result == "transport":
                raise httpx.ConnectError("Trino unavailable")
            if result == "retry" and calls == 1:
                assert not state.exists()
                raise httpx.ConnectError("Trino unavailable")
            return [{
                "days": 2 if result in {"ready", "retry"} else 0,
                "orders": 6 if result in {"ready", "retry"} else 0,
                "captured_revenue_cents": 900, "invalid_days": 0,
            }]

    monkeypatch.setattr(cli, "TrinoClient", FakeTrinoClient)
    args = [
        "complete-commerce-batch", "--batch-id", fixture.batch_id,
        "--s3-bucket", "lakehouse", "--state", str(state),
    ]
    if result == "retry":
        args.extend(["--attempts", "2", "--retry-delay-seconds", "0"])
    if result in {"ready", "retry"}:
        assert cli.main(args) == 0
        report = json.loads(capsys.readouterr().out)
        assert report["verification"]["status"] == "ready"
        assert report["checkpoint"]["created"] is True
        before = state.read_bytes()
        assert cli.main(args) == 0
        assert json.loads(capsys.readouterr().out)["checkpoint"]["created"] is False
        assert state.read_bytes() == before
    elif result == "empty":
        with pytest.raises(SystemExit, match="2"):
            cli.main(args)
        assert not state.exists()
    else:
        with pytest.raises(httpx.ConnectError, match="Trino unavailable"):
            cli.main(args)
        assert not state.exists()


def test_ingest_weather_command_lands_payload(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    valid_source_payload: dict[str, Any],
) -> None:
    FakeOpenMeteoClient.payload = valid_source_payload
    monkeypatch.setattr(cli, "OpenMeteoClient", FakeOpenMeteoClient)

    exit_code = cli.main(
        [
            "ingest-weather",
            "--name",
            "Moscow",
            "--latitude",
            "55.7558",
            "--longitude",
            "37.6173",
            "--forecast-days",
            "2",
            "--output",
            str(tmp_path),
        ]
    )

    result = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert result["created"] is True
    assert len(result["checksum"]) == 64
    assert Path(result["path"]).is_file()


def test_ingest_weather_command_lands_payload_in_s3(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    valid_source_payload: dict[str, Any],
) -> None:
    FakeOpenMeteoClient.payload = valid_source_payload
    s3_client = FakeS3Client()
    monkeypatch.setattr(cli, "OpenMeteoClient", FakeOpenMeteoClient)
    monkeypatch.setattr(cli.boto3, "client", lambda *_, **__: s3_client)

    exit_code = cli.main(
        [
            "ingest-weather",
            "--name",
            "Moscow",
            "--latitude",
            "55.7558",
            "--longitude",
            "37.6173",
            "--forecast-days",
            "2",
            "--backend",
            "s3",
            "--s3-bucket",
            "lakehouse",
            "--s3-prefix",
            "landing",
            "--s3-endpoint-url",
            "http://localhost:9000",
        ]
    )

    result = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert result["created"] is True
    assert result["path"].startswith("s3://lakehouse/landing/")
    assert len(s3_client.objects) == 1


def test_ingest_weather_command_requires_bucket_for_s3(
    monkeypatch: pytest.MonkeyPatch,
    valid_source_payload: dict[str, Any],
) -> None:
    FakeOpenMeteoClient.payload = valid_source_payload
    monkeypatch.setattr(cli, "OpenMeteoClient", FakeOpenMeteoClient)

    with pytest.raises(SystemExit) as error:
        cli.main(
            [
                "ingest-weather",
                "--name",
                "Moscow",
                "--latitude",
                "55.7558",
                "--longitude",
                "37.6173",
                "--forecast-days",
                "2",
                "--backend",
                "s3",
                "--s3-bucket",
                "",
            ]
        )

    assert error.value.code == 2


def test_ingest_weather_batch_reports_every_location(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    valid_source_payload: dict[str, Any],
) -> None:
    FakeOpenMeteoClient.payload = valid_source_payload
    monkeypatch.setattr(cli, "OpenMeteoClient", FakeOpenMeteoClient)
    manifest = tmp_path / "locations.json"
    manifest.write_text(
        json.dumps(
            {
                "locations": [
                    {"name": "Moscow", "latitude": 55.7558, "longitude": 37.6173},
                    {"name": "Berlin", "latitude": 52.52, "longitude": 13.405},
                ]
            }
        ),
        encoding="utf-8",
    )

    exit_code = cli.main(
        [
            "ingest-weather-batch",
            "--locations",
            str(manifest),
            "--forecast-days",
            "2",
            "--max-workers",
            "2",
            "--output",
            str(tmp_path / "landing"),
        ]
    )

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert report["total"] == 2
    assert report["created"] == 2
    assert [item["location"] for item in report["items"]] == ["moscow", "berlin"]


def test_ingest_weather_batch_rejects_invalid_manifest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest = tmp_path / "locations.json"
    manifest.write_text("{}", encoding="utf-8")

    with pytest.raises(SystemExit) as error:
        cli.main(["ingest-weather-batch", "--locations", str(manifest)])

    assert error.value.code == 2
    assert "must contain a 'locations' array" in capsys.readouterr().err


def test_doctor_checks_file_landing(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    exit_code = cli.main(["doctor", "--output", str(tmp_path / "landing")])

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert report["status"] == "ready"
    assert report["checks"][0]["name"] == "file_landing_write"


def test_doctor_checks_s3_bucket(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli.boto3, "client", lambda *_, **__: FakeS3Client())

    exit_code = cli.main(["doctor", "--backend", "s3", "--s3-bucket", "lakehouse"])

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert report["status"] == "ready"
    assert report["checks"][0]["target"] == "s3://lakehouse"


def test_doctor_requires_s3_bucket_versioning(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli.boto3, "client", lambda *_, **__: FakeS3Client())

    exit_code = cli.main(
        [
            "doctor",
            "--backend",
            "s3",
            "--s3-bucket",
            "lakehouse",
            "--require-versioning",
        ]
    )

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert [check["name"] for check in report["checks"]] == [
        "s3_bucket_access",
        "s3_bucket_versioning",
    ]


def test_doctor_rejects_versioning_check_for_file_backend(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as error:
        cli.main(["doctor", "--require-versioning"])

    assert error.value.code == 2
    assert "requires --backend=s3" in capsys.readouterr().err


def test_audit_landing_command_reports_integrity(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    valid_source_payload: dict[str, Any],
) -> None:
    FakeOpenMeteoClient.payload = valid_source_payload
    monkeypatch.setattr(cli, "OpenMeteoClient", FakeOpenMeteoClient)
    cli.main(
        [
            "ingest-weather",
            "--name",
            "Moscow",
            "--latitude",
            "55.7558",
            "--longitude",
            "37.6173",
            "--forecast-days",
            "2",
            "--output",
            str(tmp_path),
        ]
    )
    capsys.readouterr()

    exit_code = cli.main(["audit-landing", "--output", str(tmp_path)])

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert report["status"] == "healthy"
    assert report["valid"] == 1


def test_audit_landing_command_fails_for_empty_root(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    exit_code = cli.main(["audit-landing", "--output", str(tmp_path)])

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert report["status"] == "failed"


def test_audit_landing_command_checks_s3_objects(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    valid_source_payload: dict[str, Any],
) -> None:
    FakeOpenMeteoClient.payload = valid_source_payload
    s3_client = FakeS3Client()
    monkeypatch.setattr(cli, "OpenMeteoClient", FakeOpenMeteoClient)
    monkeypatch.setattr(cli.boto3, "client", lambda *_, **__: s3_client)
    landing_args = [
        "--backend",
        "s3",
        "--s3-bucket",
        "lakehouse",
        "--s3-prefix",
        "landing",
    ]
    cli.main(
        [
            "ingest-weather",
            "--name",
            "Moscow",
            "--latitude",
            "55.7558",
            "--longitude",
            "37.6173",
            "--forecast-days",
            "2",
            *landing_args,
        ]
    )
    capsys.readouterr()

    exit_code = cli.main(["audit-landing", *landing_args])

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert report["root"] == "s3://lakehouse/landing"
    assert report["valid"] == 1


def test_audit_landing_command_checks_s3_version_history(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    valid_source_payload: dict[str, Any],
) -> None:
    FakeOpenMeteoClient.payload = valid_source_payload
    s3_client = FakeS3Client()
    monkeypatch.setattr(cli, "OpenMeteoClient", FakeOpenMeteoClient)
    monkeypatch.setattr(cli.boto3, "client", lambda *_, **__: s3_client)
    landing_args = [
        "--backend",
        "s3",
        "--s3-bucket",
        "lakehouse",
        "--s3-prefix",
        "landing",
    ]
    cli.main(
        [
            "ingest-weather",
            "--name",
            "Moscow",
            "--latitude",
            "55.7558",
            "--longitude",
            "37.6173",
            "--forecast-days",
            "2",
            *landing_args,
        ]
    )
    capsys.readouterr()

    exit_code = cli.main(["audit-landing", *landing_args, "--include-versions"])

    report = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert report["status"] == "healthy"
    assert report["total_versions"] == 1
    assert report["items"][0]["version_id"] == "v1"


def test_audit_landing_command_rejects_versions_for_file_backend() -> None:
    with pytest.raises(SystemExit) as error:
        cli.main(["audit-landing", "--include-versions"])

    assert error.value.code == 2


def test_render_trino_access_policy_command_detects_drift(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    model = Path(__file__).parents[1] / "config" / "access" / "role-policy.json"
    output = tmp_path / "access-control-rules.json"

    check_args = [
        "render-trino-access-policy",
        "--model",
        str(model),
        "--output",
        str(output),
    ]
    assert cli.main([*check_args, "--check"]) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "drift"

    assert cli.main(check_args) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "rendered"


def test_sync_ranger_policy_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    observed: dict[str, object] = {}

    class FakeRangerAdminClient:
        def __init__(self, url: str, username: str, password: str) -> None:
            observed.update(url=url, username=username, password=password)

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def sync(self, **kwargs: object) -> dict[str, object]:
            observed.update(kwargs)
            return {"schema_version": "1.0", "status": "synchronized"}

    monkeypatch.setattr(cli, "RangerAdminClient", FakeRangerAdminClient)
    monkeypatch.setenv("RANGER_ADMIN_PASSWORD", "secret")

    exit_code = cli.main(
        [
            "sync-ranger-policy",
            "--model",
            str(Path(__file__).parents[1] / "config" / "access" / "role-policy.json"),
            "--url",
            "http://ranger.test:6080",
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)["status"] == "synchronized"
    assert observed["url"] == "http://ranger.test:6080"
    assert observed["password"] == "secret"
    assert observed["break_glass_path"] is None
    assert observed["report_schema"] == Path(
        "config/control-plane/schemas/ranger-policy-sync-report.schema.json"
    )


def test_sync_ranger_policy_requires_password(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("RANGER_ADMIN_PASSWORD", raising=False)

    with pytest.raises(SystemExit) as error:
        cli.main(
            [
                "sync-ranger-policy",
                "--model",
                str(Path(__file__).parents[1] / "config" / "access" / "role-policy.json"),
            ]
        )

    assert error.value.code == 2
    assert "RANGER_ADMIN_PASSWORD is required" in capsys.readouterr().err


def test_collect_iceberg_metadata_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    observed: dict[str, Any] = {}

    class FakeTrinoClient:
        def __init__(self, server: str, *, user: str) -> None:
            observed.update(server=server, user=user)

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    class FakeReport:
        def as_dict(self) -> dict[str, str]:
            return {"status": "ready", "schema_version": "1.0"}

    class FakeCollector:
        def __init__(self, client: FakeTrinoClient, *, schema_path: Path) -> None:
            observed["client"] = client
            observed["report_schema"] = schema_path

        def collect(self, catalog: str, schema: str, table: str) -> FakeReport:
            observed.update(catalog=catalog, schema=schema, table=table)
            return FakeReport()

    monkeypatch.setattr(cli, "TrinoClient", FakeTrinoClient)
    monkeypatch.setattr(cli, "IcebergMetadataCollector", FakeCollector)

    exit_code = cli.main(
        [
            "collect-iceberg-metadata",
            "--server",
            "http://trino.test:8080",
            "--user",
            "operator",
            "--catalog",
            "iceberg",
            "--schema",
            "ops",
            "--table",
            "events",
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == {
        "schema_version": "1.0",
        "status": "ready",
    }
    assert observed == {
        "server": "http://trino.test:8080",
        "user": "operator",
        "client": observed["client"],
        "catalog": "iceberg",
        "schema": "ops",
        "table": "events",
        "report_schema": Path(
            "config/control-plane/schemas/iceberg-metadata-report.schema.json"
        ),
    }


def test_capture_trino_baseline_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    observed: dict[str, object] = {}
    corpus = tmp_path / "queries.json"
    corpus.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "name": "cli_test",
                "queries": [
                    {"id": "scan_query", "description": "Scan", "sql": "SELECT 1"}
                ],
            }
        ),
        encoding="utf-8",
    )

    class FakeClient:
        def __init__(self, server: str, *, user: str) -> None:
            assert server == "http://trino.test:8080"
            assert user == "performance-user"

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(cli, "TrinoClient", FakeClient)

    def fake_capture(
        client: FakeClient, loaded: Any, *, schema_path: Path
    ) -> dict[str, object]:
        observed.update(client=client, schema_path=schema_path)
        return {
            "schema_version": "1.0",
            "status": "ready",
            "corpus": {"name": loaded.name, "query_count": len(loaded.queries)},
        }

    monkeypatch.setattr(cli, "capture_baseline", fake_capture)

    exit_code = cli.main(
        [
            "capture-trino-baseline",
            "--corpus",
            str(corpus),
            "--server",
            "http://trino.test:8080",
            "--user",
            "performance-user",
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == {
        "schema_version": "1.0",
        "status": "ready",
        "corpus": {"name": "cli_test", "query_count": 1},
    }
    assert observed["schema_path"] == Path(
        "config/control-plane/schemas/trino-baseline-report.schema.json"
    )


def test_capture_trino_compaction_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    observed: dict[str, object] = {}

    class FakeClient:
        def __init__(self, server: str, *, user: str) -> None:
            observed.update(server=server, user=user, client=self)

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_capture(client: object, **kwargs: object) -> dict[str, str]:
        observed.update(kwargs)
        assert client is observed["client"]
        return {"schema_version": "1.0", "status": "ready"}

    monkeypatch.setattr(cli, "TrinoClient", FakeClient)
    monkeypatch.setattr(cli, "capture_compaction_phase", fake_capture)

    exit_code = cli.main(
        [
            "capture-trino-compaction",
            "--server",
            "http://trino.test:8080",
            "--user",
            "performance-user",
            "--catalog",
            "iceberg",
            "--schema",
            "ops",
            "--table",
            "events",
            "--phase",
            "after",
            "--repetitions",
            "5",
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == {
        "schema_version": "1.0",
        "status": "ready",
    }
    assert observed == {
        "server": "http://trino.test:8080",
        "user": "performance-user",
        "client": observed["client"],
        "catalog": "iceberg",
        "schema": "ops",
        "table": "events",
        "phase": "after",
        "repetitions": 5,
    }


def test_compare_trino_compaction_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    observed: dict[str, object] = {}
    paths = [tmp_path / name for name in ("before.json", "after.json", "execution.json")]
    for index, path in enumerate(paths):
        path.write_text(json.dumps({"report": index}), encoding="utf-8")

    def fake_compare(
        before: dict[str, object],
        after: dict[str, object],
        execution: dict[str, object],
        *,
        schema_path: Path,
    ) -> dict[str, object]:
        observed["schema_path"] = schema_path
        return {
            "inputs": [before["report"], after["report"], execution["report"]]
        }

    monkeypatch.setattr(cli, "compare_compaction_phases", fake_compare)

    exit_code = cli.main(
        [
            "compare-trino-compaction",
            "--before",
            str(paths[0]),
            "--after",
            str(paths[1]),
            "--execution",
            str(paths[2]),
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == {"inputs": [0, 1, 2]}
    assert observed["schema_path"] == Path(
        "config/control-plane/schemas/trino-compaction-experiment.schema.json"
    )


def test_capture_trino_partition_pruning_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    observed: dict[str, object] = {}

    class FakeClient:
        def __init__(self, server: str, *, user: str) -> None:
            observed.update(server=server, user=user, client=self)

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_capture(client: object, **kwargs: object) -> dict[str, str]:
        observed.update(kwargs)
        assert client is observed["client"]
        return {"schema_version": "1.0", "status": "ready"}

    monkeypatch.setattr(cli, "TrinoClient", FakeClient)
    monkeypatch.setattr(cli, "capture_partition_pruning_experiment", fake_capture)

    exit_code = cli.main(
        [
            "capture-trino-partition-pruning",
            "--server",
            "http://trino.test:8080",
            "--user",
            "performance-user",
            "--catalog",
            "iceberg",
            "--schema",
            "experiments",
            "--unpartitioned-table",
            "events_flat",
            "--partitioned-table",
            "events_daily",
            "--target-day",
            "2026-01-16",
            "--repetitions",
            "5",
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == {
        "schema_version": "1.0",
        "status": "ready",
    }
    assert observed == {
        "server": "http://trino.test:8080",
        "user": "performance-user",
        "client": observed["client"],
        "catalog": "iceberg",
        "schema": "experiments",
        "unpartitioned_table": "events_flat",
        "partitioned_table": "events_daily",
        "target_day": "2026-01-16",
        "repetitions": 5,
        "report_schema": Path(
            "config/control-plane/schemas/trino-partition-pruning-experiment.schema.json"
        ),
    }


def test_capture_trino_sort_order_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    observed: dict[str, object] = {}

    class FakeClient:
        def __init__(self, server: str, *, user: str) -> None:
            observed.update(server=server, user=user, client=self)

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_capture(client: object, **kwargs: object) -> dict[str, str]:
        observed.update(kwargs)
        assert client is observed["client"]
        return {"schema_version": "1.0", "status": "ready"}

    monkeypatch.setattr(cli, "TrinoClient", FakeClient)
    monkeypatch.setattr(cli, "capture_sort_order_experiment", fake_capture)

    exit_code = cli.main(
        [
            "capture-trino-sort-order",
            "--server",
            "http://trino.test:8080",
            "--user",
            "performance-user",
            "--catalog",
            "iceberg",
            "--schema",
            "experiments",
            "--baseline-table",
            "events_random",
            "--sorted-table",
            "events_sorted",
            "--range-start",
            "1000",
            "--range-size",
            "64",
            "--repetitions",
            "5",
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == {
        "schema_version": "1.0",
        "status": "ready",
    }
    assert observed == {
        "server": "http://trino.test:8080",
        "user": "performance-user",
        "client": observed["client"],
        "catalog": "iceberg",
        "schema": "experiments",
        "baseline_table": "events_random",
        "sorted_table": "events_sorted",
        "range_start": 1000,
        "range_size": 64,
        "repetitions": 5,
        "report_schema": Path(
            "config/control-plane/schemas/trino-sort-order-experiment.schema.json"
        ),
    }


def test_verify_release_readiness_command_writes_attestation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    contract = tmp_path / "contract.json"
    evidence = tmp_path / "evidence"
    output = tmp_path / "attestation.json"
    report = {"schema_version": "1.0", "status": "ready"}
    observed: dict[str, object] = {}

    def fake_verify(
        contract_path: Path,
        evidence_root: Path,
        *,
        source_revision: str,
        schema_path: Path,
    ) -> dict[str, str]:
        observed.update(
            contract=contract_path,
            evidence_root=evidence_root,
            source_revision=source_revision,
            schema_path=schema_path,
        )
        return report

    monkeypatch.setattr(cli, "verify_release_readiness", fake_verify)

    exit_code = cli.main(
        [
            "verify-release-readiness",
            "--contract",
            str(contract),
            "--evidence-root",
            str(evidence),
            "--source-revision",
            "abc123",
            "--output",
            str(output),
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == report
    assert json.loads(output.read_text(encoding="utf-8")) == report
    assert observed == {
        "contract": contract,
        "evidence_root": evidence,
        "source_revision": "abc123",
        "schema_path": Path(
            "config/control-plane/schemas/release-readiness-attestation.schema.json"
        ),
    }


def test_verify_control_plane_contract_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    contract = tmp_path / "contract.json"
    report = {"schema_version": "1.0", "status": "compatible"}
    observed: dict[str, object] = {}

    def fake_verify(path: Path, parser: argparse.ArgumentParser) -> dict[str, str]:
        observed.update(path=path, parser=parser)
        return report

    monkeypatch.setattr(cli, "verify_control_plane_contract", fake_verify)

    exit_code = cli.main(
        ["verify-control-plane-contract", "--contract", str(contract)]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == report
    assert observed["path"] == contract
    assert isinstance(observed["parser"], argparse.ArgumentParser)
    assert observed["parser"].prog == "lakeops"


def test_verify_control_plane_contract_refreshes_schema_digests_first(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    contract = tmp_path / "contract.json"
    report = {"schema_version": "1.0", "status": "compatible"}
    observed: dict[str, object] = {}

    def fake_refresh(path: Path, parser: argparse.ArgumentParser) -> dict[str, str]:
        observed.update(path=path, parser=parser)
        return report

    monkeypatch.setattr(cli, "refresh_control_plane_schema_digests", fake_refresh)

    exit_code = cli.main(
        [
            "verify-control-plane-contract",
            "--contract",
            str(contract),
            "--refresh-schema-digests",
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == report
    assert observed["path"] == contract
    assert isinstance(observed["parser"], argparse.ArgumentParser)


def test_verify_image_lock_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    report = {"schema_version": "1.0", "status": "ready"}
    observed: dict[str, object] = {}

    def fake_verify(
        lock: Path,
        compose: Path,
        dockerfiles: list[Path],
        upgrade_plan: Path,
        schema: Path,
    ) -> dict[str, str]:
        observed.update(
            lock=lock,
            compose=compose,
            dockerfiles=dockerfiles,
            upgrade_plan=upgrade_plan,
            schema=schema,
        )
        return report

    monkeypatch.setattr(cli, "verify_image_lock", fake_verify)
    lock = tmp_path / "images.lock.json"
    compose = tmp_path / "compose.yaml"
    dockerfile = tmp_path / "Dockerfile"
    upgrade = tmp_path / "upgrade.json"
    schema = tmp_path / "report.schema.json"

    exit_code = cli.main(
        [
            "verify-image-lock",
            "--lock",
            str(lock),
            "--compose",
            str(compose),
            "--dockerfile",
            str(dockerfile),
            "--upgrade-plan",
            str(upgrade),
            "--schema",
            str(schema),
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == report
    assert observed == {
        "lock": lock,
        "compose": compose,
        "dockerfiles": [dockerfile],
        "upgrade_plan": upgrade,
        "schema": schema,
    }


def test_verify_image_lock_covers_minio_source_build_by_default(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    observed: dict[str, object] = {}

    def fake_verify(
        lock: Path,
        compose: Path,
        dockerfiles: list[Path],
        upgrade_plan: Path,
        schema: Path,
    ) -> dict[str, str]:
        observed["dockerfiles"] = dockerfiles
        return {"schema_version": "1.0", "status": "ready"}

    monkeypatch.setattr(cli, "verify_image_lock", fake_verify)

    assert cli.main(["verify-image-lock"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ready"
    assert Path("infra/minio/Dockerfile") in observed["dockerfiles"]


def test_build_release_candidate_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    report = {"schema_version": "1.0", "status": "ready"}
    observed: dict[str, object] = {}

    def fake_build(**kwargs: object) -> dict[str, str]:
        observed.update(kwargs)
        return report

    monkeypatch.setattr(cli, "build_release_candidate", fake_build)
    paths = {
        name: tmp_path / name
        for name in (
            "evidence",
            "attestation",
            "readiness",
            "control",
            "upgrade-report",
            "upgrade-plan",
            "output",
        )
    }

    exit_code = cli.main(
        [
            "build-release-candidate",
            "--evidence-root",
            str(paths["evidence"]),
            "--attestation",
            str(paths["attestation"]),
            "--readiness-contract",
            str(paths["readiness"]),
            "--control-plane-contract",
            str(paths["control"]),
            "--upgrade-report",
            str(paths["upgrade-report"]),
            "--upgrade-plan",
            str(paths["upgrade-plan"]),
            "--source-revision",
            "a" * 40,
            "--output",
            str(paths["output"]),
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == report
    assert observed == {
        "evidence_root": paths["evidence"],
        "attestation_path": paths["attestation"],
        "readiness_contract_path": paths["readiness"],
        "control_plane_contract_path": paths["control"],
        "upgrade_report_path": paths["upgrade-report"],
        "upgrade_plan_path": paths["upgrade-plan"],
        "source_revision": "a" * 40,
        "output_path": paths["output"],
        "schema_path": Path(
            "config/control-plane/schemas/release-candidate-bundle-report.schema.json"
        ),
    }


def test_verify_release_candidate_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    report = {"schema_version": "1.0", "status": "verified"}
    observed: dict[str, object] = {}

    def fake_verify(**kwargs: object) -> dict[str, str]:
        observed.update(kwargs)
        return report

    monkeypatch.setattr(cli, "verify_release_candidate", fake_verify)
    bundle = tmp_path / "bundle.tar.gz"
    candidate_report = tmp_path / "release-candidate.json"

    exit_code = cli.main(
        [
            "verify-release-candidate",
            "--bundle",
            str(bundle),
            "--report",
            str(candidate_report),
            "--expected-source-revision",
            "a" * 40,
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == report
    assert observed == {
        "bundle_path": bundle,
        "report_path": candidate_report,
        "expected_source_revision": "a" * 40,
        "report_schema_path": Path(
            "config/control-plane/schemas/release-candidate-bundle-report.schema.json"
        ),
        "verification_schema_path": Path(
            "config/control-plane/schemas/release-candidate-verification-report.schema.json"
        ),
    }


def test_plan_iceberg_maintenance_command(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    report_path = tmp_path / "metadata.json"
    report_path.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "status": "ready",
                "collected_at": "2026-08-18T13:30:00+00:00",
                "table": "lakehouse.silver.events",
                "snapshots": {
                    "current_id": "42",
                    "history": [
                        {
                            "snapshot_id": "42",
                            "committed_at": "2026-08-18T13:00:00+00:00",
                        }
                    ],
                },
                "references": [
                    {"name": "main", "reference_type": "BRANCH", "snapshot_id": "42"}
                ],
                "files": {
                    "count": 4,
                    "total_size_bytes": 4 * 128 * 1024 * 1024,
                    "delete_file_count": 0,
                },
                "manifests": {"count": 1},
            }
        ),
        encoding="utf-8",
    )

    exit_code = cli.main(["plan-iceberg-maintenance", "--input", str(report_path)])

    plan = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert plan["status"] == "healthy"
    assert plan["table"] == "lakehouse.silver.events"
    assert plan["actions"] == []

    exit_code = cli.main(
        [
            "plan-iceberg-maintenance",
            "--input",
            str(report_path),
            "--enable-orphan-inspection",
            "--orphan-retention-hours",
            "96",
            "--max-orphan-files",
            "25",
        ]
    )

    plan = json.loads(capsys.readouterr().out)
    orphan_action = next(
        action
        for action in plan["actions"]
        if action["action_type"] == "inspect_orphan_files"
    )
    assert exit_code == 0
    assert orphan_action["parameters"]["older_than"] == "2026-08-14T13:30:00+00:00"
    assert orphan_action["safety_bounds"]["max_orphan_files"] == 25


def test_plan_iceberg_maintenance_rejects_invalid_json(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    report_path = tmp_path / "metadata.json"
    report_path.write_text("not json", encoding="utf-8")

    with pytest.raises(SystemExit) as error:
        cli.main(["plan-iceberg-maintenance", "--input", str(report_path)])

    assert error.value.code == 2
    assert "Expecting value" in capsys.readouterr().err
