from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakehouse_ops import commerce_history_correction_demo


@pytest.mark.parametrize("status,code", [("ready", 0), ("failed", 2)])
def test_entrypoint_selects_history_maintenance(tmp_path, monkeypatch, capsys, status, code):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["demo", "--output", "evidence"])

    def selected(root, output, *, scenario):
        assert root == tmp_path and output == Path("evidence")
        assert scenario == "linked-history"
        return {"status": status}

    monkeypatch.setattr(commerce_history_correction_demo, "demo", selected)
    assert commerce_history_correction_demo.main() == code
    assert json.loads(capsys.readouterr().out) == {"status": status}


def test_entrypoint_rejects_unrelated_root(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["demo", "--output", "absent"])
    with pytest.raises(SystemExit) as error:
        commerce_history_correction_demo.main()
    assert error.value.code == 2
    assert not (tmp_path / "absent").exists()
