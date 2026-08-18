from __future__ import annotations

from lakehouse_ops import iceberg
from lakehouse_ops.iceberg.executor import SparkMaintenanceExecutor
from lakehouse_ops.iceberg.metadata import IcebergMetadataCollector
from lakehouse_ops.iceberg.planner import IcebergMaintenancePlanner, MaintenancePolicy


def test_public_exports_are_loaded_on_demand() -> None:
    assert iceberg.SparkMaintenanceExecutor is SparkMaintenanceExecutor
    assert iceberg.IcebergMetadataCollector is IcebergMetadataCollector
    assert iceberg.IcebergMaintenancePlanner is IcebergMaintenancePlanner
    assert iceberg.MaintenancePolicy is MaintenancePolicy


def test_unknown_export_is_rejected() -> None:
    assert not hasattr(iceberg, "unknown")
