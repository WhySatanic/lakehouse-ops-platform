"""Opt-in Docker cleanup after process exit; no Spark transaction recovery claim."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from uuid import uuid4

import pytest

from lakehouse_ops import commerce_container_cleanup as cleanup
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock

OWNER = """
import json, os, subprocess, sys
from pathlib import Path
from lakehouse_ops.commerce_run_journal import CommerceRunJournal
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock
path, lock, project, name = sys.argv[1:]
with commerce_runner_lock(Path(lock)):
    journal = CommerceRunJournal(Path(path))
    batch = {'batch_id': 'a' * 16, 'manifest_sha256': 'b' * 64}
    journal.record_container('spark', name, 'launch_intent', batch=batch, project_hint=project)
    labels = {'lakehouse.ops.run_id': journal.run_id, 'lakehouse.ops.batch_id': batch['batch_id'],
              'lakehouse.ops.manifest_sha256': batch['manifest_sha256'],
              'com.docker.compose.project': project, 'com.docker.compose.service': 'spark'}
    command = ['docker', 'run', '-d', '--name', name, '--network', 'none', '--memory', '128m']
    for key, value in labels.items():
        command += ['--label', key + '=' + value]
    command += ['--entrypoint', 'python3', 'lakehouse-ops/spark-iceberg:3.5.9-1.11.0',
                '-c', 'import time; time.sleep(180)']
    result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)
    print(json.dumps({'run_id': journal.run_id, 'container_id': result.stdout.strip()}), flush=True)
    os._exit(9)
"""


@pytest.mark.skipif(os.environ.get("LAKEOPS_RUN_CONTAINER_CLEANUP_SMOKE") != "1",
                    reason="requires local Docker and existing Spark image")
@pytest.mark.parametrize("replacement", [False, True])
def test_process_exit_cleanup_and_reused_name_do_not_touch_replacement(
    tmp_path, monkeypatch, replacement,
):
    project = "lakehouse-ops-cleanup-smoke-" + uuid4().hex
    name = "lakeops-commerce-" + uuid4().hex
    path, lock = tmp_path / "journal.sqlite3", tmp_path / "runner.lock"
    monkeypatch.setattr(cleanup, "commerce_runner_lock", lambda: commerce_runner_lock(lock))
    owner = subprocess.run([sys.executable, "-c", OWNER, str(path), str(lock), project, name],
                           capture_output=True, text=True, timeout=40, check=False)
    assert owner.returncode == 9, owner.stderr
    identity = json.loads(owner.stdout)
    ids = [identity["container_id"]]
    real_run = subprocess.run
    before = path.read_bytes()
    remove_codes = []

    def inspect(container):
        return real_run(["docker", "container", "inspect", "--", container],
                        capture_output=True, text=True, timeout=5, check=False)

    def run(command, **kwargs):
        if replacement and command[2] == "rm":
            real_run(["docker", "container", "rm", "--force", "--", ids[0]],
                     check=True, capture_output=True, timeout=10)
            created = real_run([
                "docker", "run", "-d", "--name", name, "--network", "none", "--memory", "128m",
                "--label", "com.docker.compose.project=" + project,
                "--label", "lakehouse.ops.run_id=foreign", "--entrypoint", "python3",
                "lakehouse-ops/spark-iceberg:3.5.9-1.11.0", "-c", "import time; time.sleep(180)",
            ], check=True, capture_output=True, text=True, timeout=30)
            ids.append(created.stdout.strip())
        result = real_run(command, **kwargs)
        if command[2] == "rm":
            remove_codes.append(result.returncode)
        return result

    try:
        assert json.loads(inspect(ids[0]).stdout)[0]["State"]["Running"]
        with commerce_runner_lock(lock):
            blocked = cleanup.cleanup_container(path, project=project, run_id=identity["run_id"],
                                                container_name=name, run=run)
            assert blocked["reason"] == "active_local_runner_lock"
            assert inspect(ids[0]).returncode == 0
        report = cleanup.cleanup_container(path, project=project, run_id=identity["run_id"],
                                           container_name=name, run=run)
        assert report["container_id"] == ids[0]
        if replacement:
            # Docker may acknowledge forced removal of an already absent immutable ID.
            assert report["status"] == ("incomplete" if remove_codes[0] else "ready")
            if remove_codes[0]:
                assert report["reason"] == "remove_failed"
            else:
                assert report["action"] == "removed" and inspect(ids[0]).returncode != 0
            assert json.loads(inspect(ids[1]).stdout)[0]["State"]["Running"]
            refused = cleanup.cleanup_container(path, project=project, run_id=identity["run_id"],
                                                container_name=name)
            assert refused["reason"] == "ownership_mismatch"
        else:
            assert report["status"] == "ready" and report["action"] == "removed"
            assert inspect(ids[0]).returncode != 0
            replay = cleanup.cleanup_container(path, project=project, run_id=identity["run_id"],
                                               container_name=name)
            assert replay["action"] == "already_absent"
        assert path.read_bytes() == before and lock.exists()
    finally:
        for container_id in ids:
            real_run(["docker", "container", "rm", "--force", "--", container_id],
                     check=False, capture_output=True, timeout=10)
