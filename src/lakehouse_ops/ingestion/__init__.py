from __future__ import annotations

from typing import Any

__all__ = ["Location", "OpenMeteoClient", "OpenMeteoError", "WeatherPayload"]


def __getattr__(name: str) -> Any:
    if name in {"Location", "WeatherPayload"}:
        from lakehouse_ops.ingestion import models

        return getattr(models, name)
    if name in {"OpenMeteoClient", "OpenMeteoError"}:
        from lakehouse_ops.ingestion import open_meteo

        return getattr(open_meteo, name)
    raise AttributeError(name)
