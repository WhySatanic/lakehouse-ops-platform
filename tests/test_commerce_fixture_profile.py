from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakehouse_ops.commerce_fixture_profile import main, profile_fixture_scale


def test_profiles_two_deterministic_fixture_sizes(tmp_path: Path) -> None:
    report = profile_fixture_scale(tmp_path / "scale", small_orders=100, large_orders=200)

    assert report["status"] == "ready"
    assert report["scope"] == "fixture_generation_only"
    small, large = report["profiles"]
    assert [small["orders"], large["orders"]] == [100, 200]
    assert small["batch_id"] != large["batch_id"]
    assert large["row_count"] > small["row_count"]
    assert large["jsonl_bytes"] > small["jsonl_bytes"] > 0
    assert report["jsonl_growth_ratio"] > 1
    for result in (small, large):
        assert result["generate_seconds"] >= 0
        assert result["peak_traced_python_bytes"] > 0
        manifest = json.loads(
            (tmp_path / "scale" / result["label"] / f'batch_id={result["batch_id"]}' /
             "manifest.json").read_text(encoding="utf-8")
        )
        assert result["row_count"] == sum(table["rows"] for table in manifest["tables"].values())


@pytest.mark.parametrize("small,large", [(0, 100), (100, 100), (200, 100)])
def test_rejects_invalid_order_sizes(tmp_path: Path, small: int, large: int) -> None:
    with pytest.raises(ValueError, match="order sizes"):
        profile_fixture_scale(tmp_path / "scale", small_orders=small, large_orders=large)
    assert not (tmp_path / "scale").exists()


def test_refuses_nonempty_output_root(tmp_path: Path) -> None:
    root = tmp_path / "scale"
    root.mkdir()
    sentinel = root / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")
    with pytest.raises(ValueError, match="empty output"):
        profile_fixture_scale(root, small_orders=100, large_orders=200)
    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_cli_reports_failure_without_success_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "scale"
    root.mkdir()
    (root / "keep.txt").write_text("keep", encoding="utf-8")
    assert main(["--output-root", str(root)]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "empty output" in captured.err
