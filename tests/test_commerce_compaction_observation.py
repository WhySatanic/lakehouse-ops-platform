from __future__ import annotations

import copy
import json
import sys
from contextlib import nullcontext
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from lakehouse_ops import commerce_compaction_observation as observation


@dataclass
class Query:
    rows: tuple
    query_id: str = "test-query"
    stats: dict = None


def test_capture_is_order_independent_and_retains_query_stats():
    class Client:
        reverse = False

        def query(self, sql):
            if "$files" in sql:
                return [{"files": 6, "bytes": 42}]
            if "$history" in sql:
                return [{"snapshot_id": 123}]
            return [{"order_count": n} for n in ([2, 1] if self.reverse else [1, 2])]

        def query_with_stats(self, sql):
            return Query(({"orders": 3, "revenue": 17},), stats={"cpu_time_ms": 8})

    client = Client()
    before = observation.capture(client, observation.SOURCE)
    client.reverse = True
    assert observation.capture(client, observation.SOURCE) == before
    assert before["query"]["stats"] == {"cpu_time_ms": 8}
    assert before["snapshot_id"] == "123"
    with pytest.raises(ValueError, match="unsupported"):
        observation.capture(client, "foreign_table")


@pytest.mark.parametrize("failure", [None, "copy", "rows", "revenue", "files", "snapshot",
                                     "source", "checkpoint", "process"])
def test_observe_reconciles_and_retains_failures(tmp_path, monkeypatch, failure):
    state = tmp_path / "checkpoint.json"
    state.write_bytes(b'{"processed_batches":["first","second"]}')
    original = state.read_bytes()
    source = {"rows": 62, "rows_sha256": "same", "snapshot_id": "source",
              "files": {"files": 2, "bytes": 900},
              "query": {"rows": [{"orders": 1100, "revenue": 3012053}]}}
    before = copy.deepcopy(source)
    before.update(snapshot_id="before", files={"files": 6, "bytes": 1500})
    after = copy.deepcopy(before)
    after.update(snapshot_id="after", files={"files": 1, "bytes": 600})
    source_after = copy.deepcopy(source)
    if failure == "copy":
        before["rows_sha256"] = "different"
    elif failure == "rows":
        after["rows_sha256"] = "different"
    elif failure == "revenue":
        after["query"]["rows"][0]["revenue"] += 1
    elif failure == "files":
        after["files"]["files"] = 6
    elif failure == "snapshot":
        after["snapshot_id"] = "before"
    elif failure == "source":
        source_after["snapshot_id"] = "changed"
    captures = iter([source, before, after, source_after])
    monkeypatch.setattr(observation, "capture", lambda *args: next(captures))
    stages = []

    def stage(mode, snapshot):
        stages.append((mode, snapshot))
        if mode == "compact" and failure == "checkpoint":
            state.write_bytes(b"changed by another writer")
        if mode == "compact" and failure == "process":
            raise RuntimeError("failed subprocess")

    ticks = iter([0, 4, 5, 12])
    if failure:
        with pytest.raises((ValueError, RuntimeError)):
            observation.observe(None, stage, tmp_path, state, clock=lambda: next(ticks))
    else:
        report = observation.observe(None, stage, tmp_path, state, clock=lambda: next(ticks))
        assert report["status"] == "ready"
        assert report["prepare_seconds"] == 4
        assert report["compaction_seconds"] == 7
        assert report["checkpoint"] == report["source"] == "unchanged"
    retained = json.loads((tmp_path / "compaction.json").read_text())
    assert retained["status"] == ("failed" if failure else "ready")
    assert stages == ([('prepare', None)] if failure == "copy"
                      else [('prepare', None), ('compact', 'before')])
    if failure != "checkpoint":
        assert state.read_bytes() == original


def test_unconfirmed_run_does_not_start_docker(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["compaction", "--output", str(tmp_path),
                                     "--state", str(tmp_path / "state"), "--server", "unused"])
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "lakehouse-ops-scale-demo-" + "a" * 32)
    monkeypatch.setattr(observation.subprocess, "run", lambda *a, **k: pytest.fail("Docker"))
    with pytest.raises(SystemExit):
        observation.main()


@pytest.mark.parametrize("failed", [False, True])
def test_main_uses_owned_project_checkout_plan_and_bounded_process(
    tmp_path, monkeypatch, failed,
):
    monkeypatch.chdir(tmp_path)
    state = tmp_path / "state.json"
    state.write_text("{}")
    output = tmp_path / "artifacts" / "observation"
    project = "lakehouse-ops-scale-demo-" + "a" * 32
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", project)
    monkeypatch.setenv("COMPOSE_FILE", "foreign-compose.yaml")
    monkeypatch.setattr(sys, "argv", ["compaction", "--output", str(output), "--state",
                                     str(state), "--server", "http://localhost:1234",
                                     "--confirm-diagnostic-compaction"])
    monkeypatch.setattr(observation, "commerce_runner_lock", nullcontext)
    monkeypatch.setattr(observation, "TrinoClient", lambda *a, **k: nullcontext("client"))
    metadata = SimpleNamespace(snapshots=SimpleNamespace(current_id="before"),
                               as_dict=lambda: {"status": "ready"})
    monkeypatch.setattr(observation, "IcebergMetadataCollector", lambda client: SimpleNamespace(
        collect=lambda *a: metadata,
    ))
    monkeypatch.setattr(observation, "IcebergMaintenancePlanner", lambda policy: SimpleNamespace(
        plan=lambda report: SimpleNamespace(as_dict=lambda: {"plan_id": "approved"}),
    ))
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        assert kwargs["timeout"] == 300 and kwargs["check"] is True
        assert kwargs["env"]["COMPOSE_FILE"] == str(tmp_path / "compose.yaml")
        assert kwargs["env"]["COMPOSE_PROJECT_NAME"] == project
        mode = "compact" if "COMPACTION_MODE=compact" in command else "prepare"
        (output / f"{mode}.json").write_text(json.dumps({"status": "ready"}))

    def observe(client, stage, *args):
        stage("prepare", None)
        stage("compact", "before")
        if failed:
            raise RuntimeError("post-commit verification failed")
        return {"status": "ready"}

    monkeypatch.setattr(observation.subprocess, "run", run)
    monkeypatch.setattr(observation, "observe", observe)
    assert observation.main() == (2 if failed else 0)
    assert len(commands) == 2
    assert all(c[2:4] == ["--project-name", project] and "--no-deps" in c for c in commands)
    assert "COMPACTION_EXPECTED_SNAPSHOT=before" in commands[1]
    assert json.loads((output / "plan.json").read_text()) == {"plan_id": "approved"}
    assert observation.os.environ["COMPOSE_FILE"] == "foreign-compose.yaml"


@pytest.mark.parametrize("foreign", [False, True])
def test_spark_compaction_binds_diagnostic_table_and_approved_snapshot(
    tmp_path, monkeypatch, foreign,
):
    import runpy
    from pathlib import Path

    job = runpy.run_path(str(Path("jobs/spark/commerce_compaction_observation.py")))
    plan = {"table": "lakehouse.gold.commerce_daily" if foreign else job["TABLE"],
            "plan_id": "approved", "actions": [{"action_type": "rewrite_data_files",
                                                 "action_id": "rewrite"}]}
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    report_path = tmp_path / "compact.json"
    for name, value in {"COMPACTION_CONFIRMED": "1", "COMPACTION_MODE": "compact",
                        "COMPACTION_PLAN_PATH": str(plan_path),
                        "COMPACTION_REPORT_PATH": str(report_path),
                        "COMPACTION_EXPECTED_SNAPSHOT": "before"}.items():
        monkeypatch.setenv(name, value)
    stopped = []
    spark = SimpleNamespace(sparkContext=SimpleNamespace(setLogLevel=lambda *a: None),
                            stop=lambda: stopped.append(True))
    monkeypatch.setitem(sys.modules, "spark_catalog", SimpleNamespace(
        build_session=lambda *a: spark, required_environment=observation.os.environ.__getitem__,
    ))
    calls = []

    def run(*args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(status="succeeded", as_dict=lambda: {"status": "succeeded"})

    monkeypatch.setitem(job["main"].__globals__, "SparkMaintenanceExecutor",
                        lambda executor: SimpleNamespace(run=run))
    if foreign:
        with pytest.raises(ValueError, match="diagnostic table"):
            job["main"]()
        assert not calls and not report_path.exists()
    else:
        job["main"]()
        assert calls == [{}, {"apply": True, "approved_plan_id": "approved",
                             "approved_snapshot_id": "before"}]
        assert json.loads(report_path.read_text())["status"] == "ready"
    assert stopped == [True]
