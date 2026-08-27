from importlib.metadata import version

from lakehouse_ops import __version__


def test_runtime_version_matches_distribution_metadata() -> None:
    assert __version__ == version("lakehouse-ops-platform")
