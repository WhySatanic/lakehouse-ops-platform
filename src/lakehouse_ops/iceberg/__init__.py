from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lakehouse_ops.iceberg.executor import SparkMaintenanceExecutor
    from lakehouse_ops.iceberg.metadata import IcebergMetadataCollector
    from lakehouse_ops.iceberg.planner import IcebergMaintenancePlanner, MaintenancePolicy

__all__ = [
    "IcebergMaintenancePlanner",
    "IcebergMetadataCollector",
    "MaintenancePolicy",
    "SparkMaintenanceExecutor",
]


def __getattr__(name: str) -> Any:
    if name == "SparkMaintenanceExecutor":
        from lakehouse_ops.iceberg.executor import SparkMaintenanceExecutor

        return SparkMaintenanceExecutor
    if name == "IcebergMetadataCollector":
        from lakehouse_ops.iceberg.metadata import IcebergMetadataCollector

        return IcebergMetadataCollector
    if name in {"IcebergMaintenancePlanner", "MaintenancePolicy"}:
        from lakehouse_ops.iceberg.planner import (
            IcebergMaintenancePlanner,
            MaintenancePolicy,
        )

        return {
            "IcebergMaintenancePlanner": IcebergMaintenancePlanner,
            "MaintenancePolicy": MaintenancePolicy,
        }[name]
    raise AttributeError(name)
