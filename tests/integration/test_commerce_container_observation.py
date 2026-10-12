"""Opt-in real Docker observations of one isolated, journal-recorded container."""

from __future__ import annotations

import os
import subprocess
from uuid import uuid4

import pytest

from lakehouse_ops.commerce_container_inspection import inspect_containers
from lakehouse_ops.commerce_run_journal import CommerceRunJournal


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_CONTAINER_INSPECTION_SMOKE") != "1",
    reason="requires local Docker and the reference Spark image",
)
def test_real_running_exited_absent_and_unavailable_are_distinct(tmp_path, monkeypatch):
    project = f"lakehouse-ops-container-observe-{uuid4().hex}"
    name = f"lakeops-commerce-{uuid4().hex}"
    batch = {"batch_id": "a" * 16, "manifest_sha256": "b" * 64}
    journal = CommerceRunJournal(tmp_path / "journal.sqlite3")
    journal.record_container("stage", name, "launch_intent", batch=batch, project_hint=project)
    before = journal.path.read_bytes()
    labels = {"lakehouse.ops.run_id": journal.run_id,
              "lakehouse.ops.batch_id": batch["batch_id"],
              "lakehouse.ops.manifest_sha256": batch["manifest_sha256"],
              "com.docker.compose.project": project, "com.docker.compose.service": "stage"}
    command = ["docker", "run", "-d", "--pull", "never", "--name", name,
               "--network", "none", "--memory", "128m"]
    for key, value in labels.items():
        command.extend(["--label", f"{key}={value}"])
    command.extend(["--entrypoint", "python3", "lakehouse-ops/spark-iceberg:3.5.9-1.11.0",
                    "-c", "import time; time.sleep(60)"])
    try:
        started = subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)
        report = inspect_containers(journal.path, project=project)
        assert report["counts"] == {"running": 1}
        assert report["containers"][0]["container_id"] == started.stdout.strip()
        wrong = CommerceRunJournal(tmp_path / "wrong-run.sqlite3")
        wrong.record_container("stage", name, "launch_intent", batch=batch, project_hint=project)
        assert inspect_containers(wrong.path, project=project)["counts"] == {
            "ownership_mismatch": 1,
        }
        subprocess.run(["docker", "stop", "--time", "1", name], check=True,
                       capture_output=True, timeout=10)
        assert inspect_containers(journal.path, project=project)["counts"] == {"exited": 1}
        subprocess.run(["docker", "rm", name], check=True, capture_output=True, timeout=10)
        assert inspect_containers(journal.path, project=project)["counts"] == {"absent": 1}
        with monkeypatch.context() as scoped:
            scoped.setenv("DOCKER_HOST", "tcp://127.0.0.1:9")
            failed = inspect_containers(journal.path, project=project)
            assert failed["status"] == "incomplete"
            assert failed["counts"] == {"inspect_failed": 1}
        assert journal.path.read_bytes() == before
    finally:
        subprocess.run(["docker", "rm", "--force", name], check=False,
                       capture_output=True, timeout=10)
