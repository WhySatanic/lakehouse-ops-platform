from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from lakehouse_ops.trino import TrinoQueryResult

INCIDENT = "postgresql_metastore_catalog_loss"
TABLE = "lakehouse_cache_disabled.silver.weather_hourly"
REQUIRED_TABLES = ("DBS", "SDS", "SERDES", "TBLS", "VERSION")
STATE_SQL = f"""
SELECT
  count(*) AS row_count,
  (SELECT CAST(snapshot_id AS varchar)
   FROM lakehouse_cache_disabled.silver."weather_hourly$snapshots"
   ORDER BY committed_at DESC LIMIT 1) AS snapshot_id
FROM {TABLE}
""".strip()
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class MetadataDbRecoveryError(ValueError):
    pass


class QueryClient(Protocol):
    def query_with_stats(self, sql: str) -> TrinoQueryResult: ...

    def close(self) -> None: ...


def run_metadata_db_recovery(
    client_factory: Callable[[], QueryClient],
    capture_catalog_manifest: Callable[[], dict[str, Any]],
    service_state: Callable[[], dict[str, Any]],
    stop_metastore: Callable[[], None],
    backup_database: Callable[[], dict[str, Any]],
    inject_catalog_loss: Callable[[], None],
    inspect_catalog_loss: Callable[[], dict[str, Any]],
    restore_database: Callable[[], None],
    start_metastore: Callable[[], None],
    *,
    clock: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
) -> dict[str, Any]:
    baseline_topology = _topology(service_state(), "baseline", metastore_running=True)
    baseline = {
        "catalog": _catalog_manifest(capture_catalog_manifest(), "baseline"),
        "trino": capture_trino_state(client_factory()),
        "topology": baseline_topology,
    }
    recovery_started = (monotonic or time.monotonic)()
    metastore_stopped = False
    restore_required = False
    database_restored = False

    try:
        stop_metastore()
        metastore_stopped = True
        stopped_topology = _topology(
            service_state(), "stopped", metastore_running=False
        )
        backup = _backup(backup_database())
        restore_required = True
        inject_catalog_loss()
        loss = {
            **_loss(inspect_catalog_loss()),
            "topology": _topology(
                service_state(), "loss", metastore_running=False
            ),
        }
        restore_database()
        database_restored = True
        start_metastore()
        metastore_stopped = False
    finally:
        if restore_required and not database_restored:
            restore_database()
            database_restored = True
        if metastore_stopped:
            start_metastore()

    recovery_topology = _topology(service_state(), "recovery", metastore_running=True)
    recovery = {
        "catalog": _catalog_manifest(capture_catalog_manifest(), "recovery"),
        "trino": capture_trino_state(client_factory()),
        "topology": recovery_topology,
    }
    duration = (monotonic or time.monotonic)() - recovery_started
    now = clock or (lambda: datetime.now(UTC))
    report = {
        "schema_version": "1.0",
        "status": "recovered",
        "incident": INCIDENT,
        "table": TABLE,
        "collected_at": now().astimezone(UTC).isoformat(),
        "recovery_duration_seconds": round(duration, 3),
        "backup": backup,
        "baseline": baseline,
        "stopped": {"topology": stopped_topology},
        "loss": loss,
        "recovery": recovery,
        "invariants": {
            "backup_verified": backup["required_tables"] == list(REQUIRED_TABLES),
            "catalog_loss_observed": loss["core_table_count"] == 0,
            "catalog_manifest_preserved": recovery["catalog"] == baseline["catalog"],
            "row_count_preserved": (
                recovery["trino"]["row_count"] == baseline["trino"]["row_count"]
            ),
            "snapshot_preserved": (
                recovery["trino"]["snapshot_id"] == baseline["trino"]["snapshot_id"]
            ),
            "database_container_preserved": (
                baseline_topology["database_container_id"]
                == stopped_topology["database_container_id"]
                == loss["topology"]["database_container_id"]
                == recovery_topology["database_container_id"]
            ),
            "metastore_service_restored": recovery_topology["metastore_running"],
        },
    }
    validate_metadata_db_recovery_report(report)
    return report


def capture_trino_state(client: QueryClient) -> dict[str, Any]:
    try:
        result = client.query_with_stats(STATE_SQL)
    finally:
        client.close()
    if len(result.rows) != 1:
        raise MetadataDbRecoveryError("Trino reconciliation returned an unexpected row count")
    state = {
        "query_id": result.query_id,
        "row_count": result.rows[0].get("row_count"),
        "snapshot_id": result.rows[0].get("snapshot_id"),
    }
    _trino_state(state, "captured")
    return state


def validate_metadata_db_recovery_report(report: dict[str, Any]) -> None:
    if report.get("schema_version") != "1.0" or report.get("status") != "recovered":
        raise MetadataDbRecoveryError("report is not recovered schema 1.0 evidence")
    if report.get("incident") != INCIDENT or report.get("table") != TABLE:
        raise MetadataDbRecoveryError("report incident or table is unexpected")
    if not _non_empty(report.get("collected_at")):
        raise MetadataDbRecoveryError("report collection time is missing")
    duration = report.get("recovery_duration_seconds")
    if isinstance(duration, bool) or not isinstance(duration, (int, float)) or duration < 0:
        raise MetadataDbRecoveryError("recovery duration is invalid")

    backup = _backup(report.get("backup"))
    baseline = _phase(report.get("baseline"), "baseline", metastore_running=True)
    stopped = report.get("stopped")
    if not isinstance(stopped, dict):
        raise MetadataDbRecoveryError("stopped phase is missing")
    stopped_topology = _topology(
        stopped.get("topology"), "stopped", metastore_running=False
    )
    loss = _loss(report.get("loss"))
    loss_topology = _topology(
        loss.get("topology"), "loss", metastore_running=False
    )
    recovery = _phase(report.get("recovery"), "recovery", metastore_running=True)

    if backup["required_tables"] != list(REQUIRED_TABLES):
        raise MetadataDbRecoveryError("backup does not contain every required metastore table")
    if baseline["catalog"] != recovery["catalog"]:
        raise MetadataDbRecoveryError("catalog manifest changed after restore")
    if baseline["trino"]["row_count"] != recovery["trino"]["row_count"]:
        raise MetadataDbRecoveryError("Trino row count changed after restore")
    if baseline["trino"]["snapshot_id"] != recovery["trino"]["snapshot_id"]:
        raise MetadataDbRecoveryError("Iceberg snapshot changed after restore")
    database_ids = {
        baseline["topology"]["database_container_id"],
        stopped_topology["database_container_id"],
        loss_topology["database_container_id"],
        recovery["topology"]["database_container_id"],
    }
    if len(database_ids) != 1:
        raise MetadataDbRecoveryError("metadata database container changed during restore")
    expected_invariants = {
        "backup_verified": True,
        "catalog_loss_observed": True,
        "catalog_manifest_preserved": True,
        "row_count_preserved": True,
        "snapshot_preserved": True,
        "database_container_preserved": True,
        "metastore_service_restored": True,
    }
    if report.get("invariants") != expected_invariants:
        raise MetadataDbRecoveryError("metadata database recovery invariants are incomplete")


def write_metadata_db_recovery_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _phase(value: Any, name: str, *, metastore_running: bool) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MetadataDbRecoveryError(f"{name} phase is missing")
    _catalog_manifest(value.get("catalog"), name)
    _trino_state(value.get("trino"), name)
    _topology(value.get("topology"), name, metastore_running=metastore_running)
    return value


def _backup(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MetadataDbRecoveryError("backup evidence is missing")
    if value.get("format") != "postgresql-custom":
        raise MetadataDbRecoveryError("backup format is unexpected")
    if not _non_empty(value.get("file_name")):
        raise MetadataDbRecoveryError("backup file name is missing")
    size = value.get("size_bytes")
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise MetadataDbRecoveryError("backup size is invalid")
    if not isinstance(value.get("sha256"), str) or not SHA256.fullmatch(value["sha256"]):
        raise MetadataDbRecoveryError("backup checksum is invalid")
    entries = value.get("toc_entries")
    if isinstance(entries, bool) or not isinstance(entries, int) or entries <= 0:
        raise MetadataDbRecoveryError("backup table-of-contents is empty")
    required_tables = value.get("required_tables")
    if not isinstance(required_tables, list) or not all(
        isinstance(table, str) for table in required_tables
    ):
        raise MetadataDbRecoveryError("backup required-table evidence is invalid")
    return value


def _catalog_manifest(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MetadataDbRecoveryError(f"{name} catalog manifest is missing")
    if not _non_empty(value.get("metastore_schema_version")):
        raise MetadataDbRecoveryError(f"{name} metastore schema version is missing")
    entry_count = value.get("entry_count")
    if isinstance(entry_count, bool) or not isinstance(entry_count, int) or entry_count <= 0:
        raise MetadataDbRecoveryError(f"{name} catalog entry count is invalid")
    digest = value.get("entries_sha256")
    if not isinstance(digest, str) or not SHA256.fullmatch(digest):
        raise MetadataDbRecoveryError(f"{name} catalog checksum is invalid")
    if value.get("required_tables") != list(REQUIRED_TABLES):
        raise MetadataDbRecoveryError(f"{name} catalog core tables are incomplete")
    return value


def _trino_state(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MetadataDbRecoveryError(f"{name} Trino evidence is missing")
    if not _non_empty(value.get("query_id")):
        raise MetadataDbRecoveryError(f"{name} Trino query ID is missing")
    row_count = value.get("row_count")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count <= 0:
        raise MetadataDbRecoveryError(f"{name} Trino row count is invalid")
    snapshot_id = value.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not snapshot_id.isdigit():
        raise MetadataDbRecoveryError(f"{name} Iceberg snapshot ID is invalid")
    return value


def _loss(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MetadataDbRecoveryError("catalog loss evidence is missing")
    count = value.get("core_table_count")
    if isinstance(count, bool) or not isinstance(count, int) or count != 0:
        raise MetadataDbRecoveryError("catalog loss did not remove every core table")
    return value


def _topology(value: Any, name: str, *, metastore_running: bool) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MetadataDbRecoveryError(f"{name} topology is missing")
    if value.get("metastore_running") is not metastore_running:
        raise MetadataDbRecoveryError(f"{name} Hive Metastore state is unexpected")
    if value.get("database_running") is not True:
        raise MetadataDbRecoveryError(f"{name} metadata database is not running")
    for key in ("metastore_container_id", "database_container_id"):
        _container_id(value.get(key), f"{name} {key}")
    return value


def _container_id(value: Any, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise MetadataDbRecoveryError(f"{name} is invalid")
    return value


def _non_empty(value: Any) -> bool:
    return isinstance(value, str) and bool(value)
