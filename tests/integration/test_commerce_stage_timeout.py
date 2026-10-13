"""Opt-in real Docker timeout and scoped cleanup acceptance."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops import commerce_pipeline as pipeline
from lakehouse_ops.commerce_run_journal import CommerceRunJournal


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_DOCKER_TIMEOUT_SMOKE") != "1", reason="requires local Docker",
)
def test_real_container_labels_match_prelaunch_journal(tmp_path: Path, monkeypatch) -> None:
    compose = tmp_path / "compose.yaml"
    compose.write_text(
        "services:\n  bronze-input-sync:\n"
        "    image: lakehouse-ops/spark-iceberg:3.5.9-1.11.0\n"
        "    network_mode: none\n    mem_limit: 128m\n    entrypoint: python3\n"
        "    command: [-c, 'import time; time.sleep(4)']\n", encoding="utf-8",
    )
    journal = CommerceRunJournal(tmp_path / "ownership.sqlite3")
    real_run = subprocess.run
    names = []
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "lakehouse-ops-ownership-label-smoke")

    def run(command, **kwargs):
        name = command[-2]
        names.append(name)
        with sqlite3.connect(journal.path) as db:
            assert db.execute(
                "SELECT container_name,event FROM commerce_container_events"
            ).fetchall() == [
                (name, "launch_intent"),
            ]
        command = [*command[:2], "--file", str(compose), *command[2:]]
        with subprocess.Popen(command, env=kwargs["env"], stdout=kwargs["stdout"]) as process:
            deadline = time.monotonic() + 15
            labels = None
            while time.monotonic() < deadline:
                inspected = real_run(["docker", "inspect", name], capture_output=True,
                                     text=True, timeout=5, check=False)
                if inspected.returncode == 0:
                    labels = json.loads(inspected.stdout)[0]["Config"]["Labels"]
                    break
                time.sleep(0.1)
            assert labels is not None
            assert labels["lakehouse.ops.run_id"] == journal.run_id
            assert labels["lakehouse.ops.batch_id"] == "a" * 16
            assert labels["lakehouse.ops.manifest_sha256"] == "b" * 64
            assert labels["com.docker.compose.project"] == "lakehouse-ops-ownership-label-smoke"
            assert process.wait(timeout=20) == 0

    monkeypatch.setattr(pipeline.subprocess, "run", run)
    try:
        pipeline.run_compose_stage("bronze-input-sync",
                                   {"batch_id": "a" * 16, "manifest_sha256": "b" * 64},
                                   bucket="unused", journal=journal)
        with sqlite3.connect(journal.path) as db:
            assert db.execute("SELECT event FROM commerce_container_events").fetchall() == [
                ("launch_intent",), ("completed",),
            ]
    finally:
        for name in names:
            real_run(["docker", "rm", "--force", name], check=False,
                     capture_output=True, timeout=10)


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_DOCKER_TIMEOUT_SMOKE") != "1", reason="requires local Docker",
)
@pytest.mark.parametrize("interrupted", [False, True])
def test_real_compose_timeout_removes_owned_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interrupted: bool,
) -> None:
    compose = tmp_path / "compose.yaml"
    compose.write_text(
        "services:\n  bronze-input-sync:\n"
        "    image: lakehouse-ops/spark-iceberg:3.5.9-1.11.0\n"
        "    network_mode: none\n    mem_limit: 128m\n    entrypoint: python3\n"
        "    command: [-c, 'import time; time.sleep(60)']\n",
        encoding="utf-8",
    )
    real_run = subprocess.run
    names: list[str] = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[Any]:
        if command[:2] == ["docker", "compose"]:
            names.append(command[-2])
            command = [*command[:2], "--file", str(compose), *command[2:]]
            if interrupted:
                process = subprocess.Popen(command, env=kwargs["env"], stdout=kwargs["stdout"])
                try:
                    deadline = time.monotonic() + 15
                    while time.monotonic() < deadline:
                        inspected = real_run(
                            ["docker", "inspect", names[-1]], capture_output=True,
                            text=True, check=False, timeout=5,
                        )
                        if not inspected.returncode and json.loads(inspected.stdout)[0]["State"][
                            "Running"
                        ]:
                            break
                        time.sleep(0.1)
                    else:
                        pytest.fail("owned sleeper never became active")
                finally:
                    process.kill()
                    process.wait(timeout=5)
                # Model subprocess.run propagating Ctrl+C after stopping its client;
                # the live Docker stage must be stopped by the production adapter.
                raise KeyboardInterrupt
        return real_run(command, **kwargs)

    monkeypatch.setenv("COMMERCE_STAGE_TIMEOUT_SECONDS", "3")
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "lakehouse-ops-ownership-smoke")
    monkeypatch.setattr(pipeline.subprocess, "run", run)
    journal = CommerceRunJournal(tmp_path / "ownership.sqlite3")
    try:
        for _ in range(2):
            with pytest.raises(
                KeyboardInterrupt if interrupted else pipeline.CommercePipelineError,
                match=None if interrupted else "container removed",
            ):
                pipeline.run_compose_stage(
                    "bronze-input-sync",
                    {"batch_id": "a" * 16, "manifest_sha256": "b" * 64}, bucket="unused",
                    journal=journal,
                )
            inspected = real_run(
                ["docker", "inspect", names[-1]], check=False,
                capture_output=True, text=True, timeout=10,
            )
            assert inspected.returncode != 0
            assert "no such object" in inspected.stderr.lower()
        assert len(set(names)) == 2
        with sqlite3.connect(journal.path) as db:
            assert db.execute(
                "SELECT container_name,event FROM commerce_container_events ORDER BY sequence"
            ).fetchall() == [(name, event) for name in names
                            for event in (
                                "launch_intent",
                                "interrupted_removed" if interrupted else "timeout_removed",
                            )]
    finally:
        for name in names:
            real_run(
                ["docker", "rm", "--force", name], check=False, capture_output=True, timeout=10,
            )
