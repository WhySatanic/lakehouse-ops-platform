from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from lakehouse_ops import commerce_pipeline_profile_demo
from lakehouse_ops.commerce_linked_demo import demo


@pytest.mark.parametrize("status,code", [("ready", 0), ("failed", 2)])
def test_entrypoint_selects_profile_and_passes_volumes(tmp_path, monkeypatch, capsys, status, code):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["demo", "--output", "evidence", "--orders", "200", "2000"])

    def selected(root, output, *, scenario, orders):
        assert root == tmp_path and output == Path("evidence")
        assert scenario == "profile" and orders == (200, 2000)
        return {"status": status}

    monkeypatch.setattr(commerce_pipeline_profile_demo, "demo", selected)
    assert commerce_pipeline_profile_demo.main() == code
    assert json.loads(capsys.readouterr().out) == {"status": status}


@pytest.mark.parametrize("orders", [(99, 1000), (100, 100), (1000, 100), (100, 10001), (100,)])
def test_invalid_volume_does_not_create_output_or_start_docker(tmp_path, orders):
    with pytest.raises(ValueError, match="ascending"):
        demo(tmp_path, tmp_path / "absent", scenario="profile", orders=orders)
    assert not (tmp_path / "absent").exists()


def test_orders_cannot_change_existing_scenarios(tmp_path):
    with pytest.raises(ValueError, match="only supported"):
        demo(tmp_path, tmp_path / "absent", orders=(100, 1000))
    assert not (tmp_path / "absent").exists()


def test_missing_memory_overlay_does_not_create_output(tmp_path):
    module = tmp_path / "src/lakehouse_ops/commerce_pipeline_profile.py"
    module.parent.mkdir(parents=True)
    module.touch()
    with pytest.raises(ValueError, match="memory Compose overlay"):
        demo(tmp_path, tmp_path / "absent", scenario="profile")
    assert not (tmp_path / "absent").exists()


def test_custom_volumes_reach_only_measurement_child(tmp_path):
    module = tmp_path / "src/lakehouse_ops/commerce_pipeline_profile.py"
    module.parent.mkdir(parents=True)
    module.touch()
    (tmp_path / "compose.spark-memory.yaml").touch()
    output = tmp_path / "evidence"
    children = []

    def run(command, **kwargs):
        if command[1] == "-m":
            assert command[2] == "lakehouse_ops.commerce_pipeline_profile"
            assert command[-3:] == ["--orders", "200", "2000"]
            children.append(command)
            acceptance = output / "acceptance"
            acceptance.mkdir()
            (acceptance / "profile.json").write_text(json.dumps({"status": "ready"}))
        else:
            assert "--orders" not in command
        return SimpleNamespace(returncode=0)

    assert demo(tmp_path, output, run=run, scenario="profile",
                orders=(200, 2000))["status"] == "ready"
    assert len(children) == 1


def test_entrypoint_rejects_unrelated_root(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["demo", "--output", "absent"])
    with pytest.raises(SystemExit) as error:
        commerce_pipeline_profile_demo.main()
    assert error.value.code == 2
    assert not (tmp_path / "absent").exists()
