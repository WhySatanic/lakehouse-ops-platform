from importlib.metadata import version

import pytest

import lakehouse_ops
from lakehouse_ops import __version__


def test_runtime_version_matches_distribution_metadata() -> None:
    assert __version__ == version("lakehouse-ops-platform")


def test_runtime_version_has_safe_source_only_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_distribution(_name: str) -> str:
        raise lakehouse_ops.PackageNotFoundError

    monkeypatch.setattr(lakehouse_ops, "version", missing_distribution)

    assert lakehouse_ops._resolve_version() == "0+unknown"
