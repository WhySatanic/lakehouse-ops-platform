from __future__ import annotations

import json
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import pytest

from lakehouse_ops import cli
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

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]
        return {"ETag": '"test"'}

    def head_bucket(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["Bucket"] == "lakehouse"
        return {}


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
        def __init__(self, client: FakeTrinoClient) -> None:
            observed["client"] = client

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
    }


def test_capture_trino_baseline_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
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
    monkeypatch.setattr(
        cli,
        "capture_baseline",
        lambda client, loaded: {
            "schema_version": "1.0",
            "status": "ready",
            "corpus": {"name": loaded.name, "query_count": len(loaded.queries)},
        },
    )

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
    paths = [tmp_path / name for name in ("before.json", "after.json", "execution.json")]
    for index, path in enumerate(paths):
        path.write_text(json.dumps({"report": index}), encoding="utf-8")

    monkeypatch.setattr(
        cli,
        "compare_compaction_phases",
        lambda before, after, execution: {
            "inputs": [before["report"], after["report"], execution["report"]]
        },
    )

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
