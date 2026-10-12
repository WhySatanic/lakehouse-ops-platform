from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

import pytest

from lakehouse_ops.commerce_container_inspection import inspect_containers, main
from lakehouse_ops.commerce_run_journal import CommerceRunJournal


def recorded(tmp_path, *, project="lakehouse-ops-observe", name="lakeops-commerce-known"):
    journal = CommerceRunJournal(tmp_path / "journal.sqlite3")
    batch = {"batch_id": "a" * 16, "manifest_sha256": "b" * 64}
    journal.record_container("spark", name, "launch_intent", batch=batch, project_hint=project)
    return journal, {"Name": f"/{name}", "Id": "docker-id", "State": {"Status": "running"},
                     "Config": {"Labels": {
                         "lakehouse.ops.run_id": journal.run_id,
                         "lakehouse.ops.batch_id": batch["batch_id"],
                         "lakehouse.ops.manifest_sha256": batch["manifest_sha256"],
                         "com.docker.compose.project": project,
                         "com.docker.compose.service": "spark",
                     }}}


@pytest.mark.parametrize("state", ["running", "exited", "created", "paused", "restarting",
                                  "dead", "removing"])
def test_owned_state_is_observed_read_only(tmp_path, state):
    journal, container = recorded(tmp_path)
    container["State"]["Status"] = state
    before = journal.path.read_bytes()

    def run(command, **kwargs):
        assert command == ["docker", "container", "inspect", "--", "lakeops-commerce-known"]
        assert kwargs == {"capture_output": True, "text": True, "check": False, "timeout": 5}
        return SimpleNamespace(returncode=0, stdout=json.dumps([container]))

    report = inspect_containers(journal.path, project="lakehouse-ops-observe", run=run)
    assert report["status"] == "observed"
    assert report["counts"] == {state: 1}
    assert not report["has_more"]
    assert report["containers"][0]["container_id"] == "docker-id"
    assert journal.path.read_bytes() == before


@pytest.mark.parametrize("key", ["lakehouse.ops.run_id", "lakehouse.ops.batch_id",
                                 "lakehouse.ops.manifest_sha256", "com.docker.compose.project",
                                 "com.docker.compose.service", "name"])
def test_reused_name_or_identity_is_not_claimed_owned(tmp_path, key):
    journal, container = recorded(tmp_path)
    if key == "name":
        container["Name"] = "/another-container"
    else:
        container["Config"]["Labels"][key] = "different"
    report = inspect_containers(journal.path, project="lakehouse-ops-observe",
                                run=lambda *a, **kw: SimpleNamespace(
                                    returncode=0, stdout=json.dumps([container])))
    assert report["counts"] == {"ownership_mismatch": 1}
    assert "container_id" not in report["containers"][0]


@pytest.mark.parametrize("error", ["absent", "daemon", "timeout", "launch", "json",
                                    "empty", "state", "structure", "labels"])
def test_uncertain_docker_response_is_not_mistaken_for_absence(tmp_path, error):
    journal, container = recorded(tmp_path)

    def run(*args, **kwargs):
        if error == "timeout":
            raise subprocess.TimeoutExpired(args[0], 5)
        if error == "launch":
            raise FileNotFoundError("docker")
        if error in {"absent", "daemon"}:
            return SimpleNamespace(returncode=1, stderr=(
                "Error: No such container: lakeops-commerce-known\n" if error == "absent"
                else "Cannot connect to Docker daemon"))
        if error == "state":
            container["State"]["Status"] = "unknown"
        if error == "structure":
            container.pop("State")
        if error == "labels":
            container["Config"]["Labels"] = 4
        return SimpleNamespace(returncode=0, stdout=("not json" if error == "json" else
                               "[]" if error == "empty" else json.dumps([container])))

    report = inspect_containers(journal.path, project="lakehouse-ops-observe", run=run)
    assert report["counts"] == {"absent" if error == "absent" else "inspect_failed": 1}
    assert report["status"] == ("observed" if error == "absent" else "incomplete")


def test_project_and_limit_filter_only_recorded_names(tmp_path):
    journal, _ = recorded(tmp_path)
    recorded(tmp_path, project="another-project", name="another-container")
    recorded(tmp_path, name="lakeops-commerce-newer")
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=1,
                               stderr="Error: No such container: lakeops-commerce-newer")

    report = inspect_containers(journal.path, project="lakehouse-ops-observe", limit=1, run=run)
    assert report["has_more"]
    assert len(commands) == 1
    assert commands[0][-1] == "lakeops-commerce-newer"


@pytest.mark.parametrize("limit", [0, 101, True])
def test_bad_limit_rejected_without_creating_journal(tmp_path, limit):
    with pytest.raises(ValueError):
        inspect_containers(tmp_path / "missing", project="test", limit=limit)
    assert not (tmp_path / "missing").exists()


def test_missing_journal_and_empty_project_fail_without_writing(tmp_path, capsys):
    missing = tmp_path / "missing"
    with pytest.raises(ValueError):
        inspect_containers(missing, project="")
    assert main(["--journal", str(missing), "--project", "test"]) == 2
    assert "cannot inspect" in capsys.readouterr().err
    assert not missing.exists()


def test_actual_docker_daemon_missing_response(tmp_path):
    journal, _ = recorded(tmp_path)
    report = inspect_containers(journal.path, project="lakehouse-ops-observe",
                                run=lambda *a, **kw: SimpleNamespace(returncode=1, stderr=(
                                    "Error response from daemon: No such container: "
                                    "lakeops-commerce-known\n")))
    assert report["counts"] == {"absent": 1}


@pytest.mark.parametrize("status,code", [("observed", 0), ("incomplete", 2)])
def test_entrypoint_retains_partial_report(monkeypatch, capsys, status, code):
    monkeypatch.setattr("lakehouse_ops.commerce_container_inspection.inspect_containers",
                        lambda *a, **kw: {"status": status})
    assert main(["--journal", "journal.sqlite3", "--project", "test"]) == code
    assert json.loads(capsys.readouterr().out)["status"] == status
