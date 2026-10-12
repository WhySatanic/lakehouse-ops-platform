from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from lakehouse_ops import commerce_pipeline_profile as profile
from lakehouse_ops.trino import TrinoQueryStats


def test_memory_parser_ignores_other_stage_output():
    line = json.dumps({"app_name": "test", "kind": "spark_container_memory"})
    assert profile.memory_reports("Spark INFO\n" + line + "\n{}")[0]["app_name"] == "test"


def test_execute_retains_failed_process_logs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def run(command, **kwargs):
        assert command[-2:] == ["--s3-bucket", "lakehouse"]
        kwargs["stderr"].write("failed stage")
        raise subprocess.CalledProcessError(2, command)

    monkeypatch.setattr(profile.subprocess, "run", run)
    with pytest.raises(subprocess.CalledProcessError):
        profile.execute(["--s3-bucket", "lakehouse"], tmp_path, "failed")
    assert (tmp_path / "failed.stderr.log").read_text() == "failed stage"


@pytest.mark.parametrize("volumes", [[99, 100], [100, 100], [100]])
def test_invalid_volume_does_not_start_pipeline(tmp_path: Path, volumes):
    with pytest.raises(ValueError, match="ascending"):
        profile.profile(tmp_path, orders=volumes, endpoint="unused", server="unused")


def test_default_project_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("COMPOSE_PROJECT_NAME", raising=False)
    with pytest.raises(ValueError, match="dedicated"):
        profile.profile(tmp_path, orders=[100, 1000], endpoint="unused", server="unused")


@pytest.mark.parametrize("failure", [None, "memory", "idle", "existing", "second-run"])
def test_two_volume_evidence_or_retained_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure,
):
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "lakehouse-ops-scale-test")
    running = failure == "existing"

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def query(self, sql):
            if "information_schema" in sql:
                return [{"table_schema": "gold", "table_name": "commerce_daily"}] if running else []
            assert '"gold"."commerce_daily$files"' in sql
            return [{"files": 1, "bytes": 1024}]

        def query_with_stats(self, sql):
            return SimpleNamespace(query_id="q1", rows=({"revenue": 100},),
                                   stats=TrinoQueryStats("FINISHED", 1, 1, 1, 1, 1, 1, 1, 0))

    def execute(args, output, label):
        nonlocal running
        assert "--s3-bucket" in args
        if "landing" in label:
            return {}
        if "idle" in label:
            return {"status": "ready" if failure == "idle" else "idle"}
        if failure == "second-run" and label == "sample-2":
            raise subprocess.CalledProcessError(2, args)
        running = True
        fixture = next((output / label).iterdir())
        batch = json.loads((fixture / "manifest.json").read_text())["batch_id"]
        observations = [{"app_name": str(i), "kind": "spark_container_memory",
                         "status": "unavailable" if failure == "memory" else "observed"}
                        for i in range(7)]
        (output / f"{label}.stderr.log").write_text(
            "✓ Docker stage completed\n" + "\n".join(json.dumps(row) for row in observations),
            encoding="utf-8",
        )
        return {"status": "ready", "batch_id": batch}

    monkeypatch.setattr(profile, "TrinoClient", Client)
    monkeypatch.setattr(profile, "execute", execute)
    if failure:
        with pytest.raises((ValueError, subprocess.CalledProcessError)):
            profile.profile(tmp_path, orders=[100, 1000], endpoint="unused", server="unused")
        assert json.loads((tmp_path / "profile.json").read_text())["status"] == "failed"
        if failure == "second-run":
            evidence = json.loads((tmp_path / "profile.json").read_text())
            assert len(evidence["samples"]) == 1
            assert evidence["samples"][0]["orders"] == 100
    else:
        result = profile.profile(tmp_path, orders=[100, 1000], endpoint="unused", server="unused")
        assert result["status"] == "ready"
        assert [s["orders"] for s in result["samples"]] == [100, 1000]
        assert result["samples"][0]["idle_replay"] == "verified"
        assert result["samples"][0]["cumulative_current_files"][0]["bytes"] == 1024
