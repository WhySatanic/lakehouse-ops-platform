from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

import pytest

from lakehouse_ops import commerce_container_cleanup as cleanup
from lakehouse_ops.commerce_run_journal import CommerceRunJournal
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock


@pytest.mark.parametrize("failure", [None, "absent", "unknown", "duplicate", "mismatch",
                                     "id", "remove", "timeout", "confirm", "daemon"])
def test_cleanup_uses_owned_immutable_id_and_preserves_journal(tmp_path, monkeypatch, failure):
    journal = CommerceRunJournal(tmp_path / "journal.sqlite3")
    project = "lakehouse-ops-cleanup-unit"
    name = "lakeops-commerce-" + "a" * 32
    identity = "b" * 64
    batch = {"batch_id": "c" * 16, "manifest_sha256": "d" * 64}
    for _ in range(2 if failure == "duplicate" else 0 if failure == "unknown" else 1):
        journal.record_container("spark", name, "launch_intent", batch=batch, project_hint=project)
    if failure == "unknown":
        journal.record_container("spark", "other", "launch_intent",
                                 batch=batch, project_hint=project)
    before = journal.path.read_bytes()
    monkeypatch.setattr(cleanup, "commerce_runner_lock",
                        lambda: commerce_runner_lock(tmp_path / "lock"))
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        assert kwargs["timeout"] in {5, 30}
        if command[2] == "rm":
            assert command == ["docker", "container", "rm", "--force", "--", identity]
            if failure == "timeout":
                raise subprocess.TimeoutExpired(command, 30)
            return SimpleNamespace(returncode=int(failure == "remove"))
        target = command[-1]
        if target == identity or failure == "absent":
            return SimpleNamespace(returncode=int(failure != "confirm"), stdout="[]",
                                   stderr="cannot connect to daemon" if failure == "daemon" else
                                   f"Error response from daemon: No such container: {target}")
        labels = {"lakehouse.ops.run_id": "foreign" if failure == "mismatch" else journal.run_id,
                  "lakehouse.ops.batch_id": batch["batch_id"],
                  "lakehouse.ops.manifest_sha256": batch["manifest_sha256"],
                  "com.docker.compose.project": project, "com.docker.compose.service": "spark"}
        data = {"Name": f"/{name}", "Id": "--all" if failure == "id" else identity,
                "State": {"Status": "running"}, "Config": {"Labels": labels}}
        return SimpleNamespace(returncode=0, stdout=json.dumps([data]))

    report = cleanup.cleanup_container(journal.path, project=project, run_id=journal.run_id,
                                       container_name=name, run=run)
    if failure in {None, "absent"}:
        assert report["status"] == "ready"
        assert report["action"] == ("already_absent" if failure else "removed")
    elif failure in {"remove", "timeout", "confirm", "daemon"}:
        assert report["status"] == "incomplete"
    else:
        assert report["status"] == "refused"
    if failure in {"unknown", "duplicate", "mismatch", "id", "absent"}:
        assert all(command[2] != "rm" for command in commands)
    assert journal.path.read_bytes() == before


def test_active_local_runner_blocks_even_inspection(tmp_path, monkeypatch):
    lock = tmp_path / "lock"
    monkeypatch.setattr(cleanup, "commerce_runner_lock", lambda: commerce_runner_lock(lock))
    with commerce_runner_lock(lock):
        report = cleanup.cleanup_container(
            tmp_path / "missing", project="lakehouse-ops-cleanup-unit", run_id="a" * 32,
            container_name="lakeops-commerce-" + "b" * 32,
            run=lambda *a, **kw: pytest.fail("Docker must not run while owner lock is held"),
        )
    assert report["status"] == "refused" and report["reason"] == "active_local_runner_lock"
    assert not (tmp_path / "missing").exists()
    assert lock.exists()


@pytest.mark.parametrize("field,value", [("project", "unrelated"), ("run_id", "--all"),
                                       ("container_name", "foreign")])
def test_invalid_explicit_target_never_reaches_docker(tmp_path, field, value):
    values = {"project": "lakehouse-ops-cleanup-unit", "run_id": "a" * 32,
              "container_name": "lakeops-commerce-" + "b" * 32, field: value}
    with pytest.raises(ValueError):
        cleanup.cleanup_container(tmp_path / "missing", **values,
                                  run=lambda *a, **kw: pytest.fail("no Docker"))


@pytest.mark.parametrize("confirmed,root", [(False, True), (True, False)])
def test_cli_requires_explicit_stop_and_same_checkout(tmp_path, monkeypatch, confirmed, root):
    if not root:
        monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cleanup, "cleanup_container", lambda *a, **kw: pytest.fail("no mutation"))
    argv = ["--journal", "unused", "--project", "lakehouse-ops-test", "--run-id", "a" * 32,
            "--container", "lakeops-commerce-" + "b" * 32]
    with pytest.raises(SystemExit) as error:
        cleanup.main([*argv, *(["--confirm-stop"] if confirmed else [])])
    assert error.value.code == 2


@pytest.mark.parametrize("status,code", [("ready", 0), ("refused", 2), ("incomplete", 2)])
def test_cli_returns_evidence_and_nonzero_on_refusal(monkeypatch, capsys, status, code):
    monkeypatch.setattr(cleanup, "cleanup_container", lambda *a, **kw: {"status": status})
    assert cleanup.main(["--journal", "unused", "--project", "lakehouse-ops-test",
                         "--run-id", "a" * 32, "--container", "lakeops-commerce-" + "b" * 32,
                         "--confirm-stop"]) == code
    assert json.loads(capsys.readouterr().out)["status"] == status
