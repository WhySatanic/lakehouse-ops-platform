from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakehouse_ops import commerce_owner_exit_demo


@pytest.mark.parametrize("status,code", [("ready", 0), ("failed", 2)])
def test_entrypoint_selects_scoped_owner_exit(tmp_path, monkeypatch, capsys, status, code):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["demo", "--output", "evidence"])

    def demo(root, output, *, scenario):
        assert root == tmp_path
        assert output == Path("evidence")
        assert scenario == "owner-exit"
        return {"status": status}

    monkeypatch.setattr(commerce_owner_exit_demo, "demo", demo)
    assert commerce_owner_exit_demo.main() == code
    assert json.loads(capsys.readouterr().out) == {"status": status}


def test_entrypoint_rejects_unrelated_root(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["demo", "--output", "evidence"])
    with pytest.raises(SystemExit) as error:
        commerce_owner_exit_demo.main()
    assert error.value.code == 2
    assert not (tmp_path / "evidence").exists()
