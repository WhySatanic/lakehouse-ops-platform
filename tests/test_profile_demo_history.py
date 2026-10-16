from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from lakehouse_ops import commerce_pipeline_profile_demo as entry
from lakehouse_ops.commerce_linked_demo import demo


@pytest.mark.parametrize("options", [
    {"customers": 1}, {"customers": 100001}, {"customers": True},
    {"batches": 1}, {"batches": 101}, {"batches": 2.5},
    {"customer_revisions": [0]}, {"customer_revisions": [0, True]},
    {"customer_revisions": [0, -1]}, {"customer_revisions": [0, 10001]},
])
def test_invalid_workload_starts_nothing_and_creates_no_output(tmp_path, options):
    with pytest.raises(ValueError):
        demo(tmp_path, tmp_path / "absent", scenario="profile", **options)
    assert not (tmp_path / "absent").exists()


@pytest.mark.parametrize("options", [{"customers": 10}, {"batches": 2},
                                  {"customer_revisions": [0, 1]}])
def test_history_options_cannot_change_recovery_scenarios(tmp_path, options):
    with pytest.raises(ValueError, match="only supported"):
        demo(tmp_path, tmp_path / "absent", **options)
    assert not (tmp_path / "absent").exists()


@pytest.mark.parametrize("fault", [None, "child-timeout", "bad-history"])
@pytest.mark.parametrize("revisions", [None, [0, 1, 1, 2]])
def test_four_snapshot_workload_reaches_one_child_and_keeps_budget_and_owned_cleanup(
    tmp_path, fault, revisions,
):
    module = tmp_path / "src/lakehouse_ops/commerce_pipeline_profile.py"
    module.parent.mkdir(parents=True)
    module.touch()
    (tmp_path / "compose.spark-memory.yaml").touch()
    output = tmp_path / "evidence"
    children, stops = [], []

    def run(command, **kwargs):
        if command[1] == "-m":
            children.append(command)
            assert kwargs["timeout"] == 3600
            expected = ["--customers", "10000", "--batches", "4"]
            if revisions is not None:
                expected += ["--customer-revisions", "0", "1", "1", "2"]
            assert command[-len(expected):] == expected
            if fault == "child-timeout":
                import subprocess
                raise subprocess.TimeoutExpired(command, kwargs["timeout"])
            acceptance = output / "acceptance"
            acceptance.mkdir()
            (acceptance / "profile.json").write_text(json.dumps({
                "status": "failed" if fault else "ready", "samples": [{"batch": 1}],
            }))
        if "down" in command:
            assert "--project-name" in command
            assert "--volumes" not in command and "--rmi" not in command
            stops.append(command)
        if command[1:3] == ["network", "inspect"]:
            return SimpleNamespace(returncode=0, stdout=json.dumps([{
                "Name": command[-1], "Containers": {},
                "Labels": {"com.docker.compose.project": kwargs["env"]["COMPOSE_PROJECT_NAME"]},
            }]))
        return SimpleNamespace(returncode=0)

    report = demo(tmp_path, output, run=run, scenario="profile", orders=(100, 100),
                  customers=10000, batches=4, customer_revisions=revisions)
    assert report["status"] == ("failed" if fault else "ready")
    assert report["scope"] == ("customer_revision_history_fresh_reference_stack" if revisions else
                               "sequential_snapshot_history_fresh_reference_stack")
    assert report["workload"] == {"customers": 10000, "batches": 4,
                                  "customer_revisions": revisions}
    assert len(children) == len(stops) == 1
    assert json.loads((output / "demo.json").read_text()) == report
    if fault == "bad-history":
        assert json.loads((output / "acceptance/profile.json").read_text())["samples"]


def test_entrypoint_passes_explicit_history_options(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["demo", "--output", "evidence", "--customers", "10000",
                                    "--batches", "4", "--customer-revisions", "0", "1", "1", "2"])
    def selected(root, output, **kwargs):
        assert kwargs == {"scenario": "profile", "orders": (100, 1000),
                          "customers": 10000, "batches": 4, "customer_revisions": [0, 1, 1, 2]}
        return {"status": "ready"}
    monkeypatch.setattr(entry, "demo", selected)
    assert entry.main() == 0
