from importlib.metadata import PackageNotFoundError, version


def _resolve_version() -> str:
    try:
        return version("lakehouse-ops-platform")
    except PackageNotFoundError:
        return "0+unknown"


__version__ = _resolve_version()
