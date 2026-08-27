from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx

from lakehouse_ops.trino import TrinoProtocolError, TrinoQueryError, TrinoQueryResult

INCIDENT = "hive_metastore_unavailable"
TABLE = "lakehouse_cache_disabled.silver.weather_hourly"
STATE_SQL = f"""
SELECT
  (SELECT count(*) FROM {TABLE}) AS row_count,
  (SELECT CAST(snapshot_id AS varchar)
   FROM lakehouse_cache_disabled.silver."weather_hourly$snapshots"
   ORDER BY committed_at DESC LIMIT 1) AS snapshot_id
""".strip()


class MetastoreRecoveryError(ValueError):
    pass


class QueryClient(Protocol):
    def query_with_stats(self, sql: str) -> TrinoQueryResult: ...

    def close(self) -> None: ...


def run_metastore_recovery(
    client_factory: Callable[[], QueryClient],
    service_state: Callable[[], dict[str, Any]],
    stop_metastore: Callable[[], None],
    start_metastore: Callable[[], None],
    *,
    clock: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
) -> dict[str, Any]:
    baseline_topology = _topology(service_state(), metastore_running=True)
    baseline = _capture_state(client_factory())
    restore_required = False
    outage_started = (monotonic or time.monotonic)()
    outage: dict[str, Any]

    try:
        restore_required = True
        stop_metastore()
        outage_topology = _topology(service_state(), metastore_running=False)
        client = client_factory()
        try:
            client.query_with_stats(STATE_SQL)
        except (TrinoQueryError, TrinoProtocolError, httpx.HTTPError) as error:
            outage = {
                "metastore_query_failed": True,
                "error_type": type(error).__name__,
                "error": str(error),
                "topology": outage_topology,
            }
        else:
            raise MetastoreRecoveryError(
                "cache-disabled Trino query succeeded while Hive Metastore was stopped"
            )
        finally:
            client.close()
    finally:
        if restore_required:
            start_metastore()

    recovery_topology = _topology(service_state(), metastore_running=True)
    recovery = _capture_state(client_factory())
    duration = (monotonic or time.monotonic)() - outage_started
    now = clock or (lambda: datetime.now(UTC))
    report = {
        "schema_version": "1.0",
        "status": "recovered",
        "incident": INCIDENT,
        "table": TABLE,
        "collected_at": now().astimezone(UTC).isoformat(),
        "outage_duration_seconds": round(duration, 3),
        "baseline": {**baseline, "topology": baseline_topology},
        "outage": outage,
        "recovery": {**recovery, "topology": recovery_topology},
        "invariants": {
            "row_count_preserved": recovery["row_count"] == baseline["row_count"],
            "snapshot_preserved": recovery["snapshot_id"] == baseline["snapshot_id"],
            "metadata_db_container_preserved": (
                baseline_topology["database_container_id"]
                == outage["topology"]["database_container_id"]
                == recovery_topology["database_container_id"]
            ),
        },
    }
    validate_metastore_recovery_report(report)
    return report


def validate_metastore_recovery_report(report: dict[str, Any]) -> None:
    if report.get("schema_version") != "1.0" or report.get("status") != "recovered":
        raise MetastoreRecoveryError("report is not recovered schema 1.0 evidence")
    if report.get("incident") != INCIDENT or report.get("table") != TABLE:
        raise MetastoreRecoveryError("report incident or table is unexpected")
    if not isinstance(report.get("collected_at"), str) or not report["collected_at"]:
        raise MetastoreRecoveryError("report collection time is missing")
    duration = report.get("outage_duration_seconds")
    if isinstance(duration, bool) or not isinstance(duration, (int, float)) or duration < 0:
        raise MetastoreRecoveryError("outage duration is invalid")

    baseline = _report_phase(report.get("baseline"), "baseline")
    recovery = _report_phase(report.get("recovery"), "recovery")
    outage = report.get("outage")
    if not isinstance(outage, dict):
        raise MetastoreRecoveryError("outage evidence is missing")
    if outage.get("metastore_query_failed") is not True:
        raise MetastoreRecoveryError("outage query did not fail")
    if not isinstance(outage.get("error_type"), str) or not outage["error_type"]:
        raise MetastoreRecoveryError("outage error type is missing")
    if not isinstance(outage.get("error"), str) or not outage["error"]:
        raise MetastoreRecoveryError("outage error is missing")

    baseline_topology = _topology(baseline["topology"], metastore_running=True)
    outage_topology = _topology(outage.get("topology"), metastore_running=False)
    recovery_topology = _topology(recovery["topology"], metastore_running=True)
    if baseline["row_count"] != recovery["row_count"]:
        raise MetastoreRecoveryError("row count changed after recovery")
    if baseline["snapshot_id"] != recovery["snapshot_id"]:
        raise MetastoreRecoveryError("snapshot changed after recovery")
    database_ids = {
        baseline_topology["database_container_id"],
        outage_topology["database_container_id"],
        recovery_topology["database_container_id"],
    }
    if len(database_ids) != 1:
        raise MetastoreRecoveryError("metadata database container changed during outage")
    if report.get("invariants") != {
        "row_count_preserved": True,
        "snapshot_preserved": True,
        "metadata_db_container_preserved": True,
    }:
        raise MetastoreRecoveryError("recovery invariants are incomplete")


def write_metastore_recovery_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _capture_state(client: QueryClient) -> dict[str, Any]:
    try:
        result = client.query_with_stats(STATE_SQL)
    finally:
        client.close()
    if len(result.rows) != 1:
        raise MetastoreRecoveryError("recovery query returned an unexpected row count")
    row = result.rows[0]
    row_count = row.get("row_count")
    snapshot_id = row.get("snapshot_id")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count <= 0:
        raise MetastoreRecoveryError("recovery query row count is invalid")
    if not isinstance(snapshot_id, str) or not snapshot_id.isdigit():
        raise MetastoreRecoveryError("recovery query snapshot ID is invalid")
    if not isinstance(result.query_id, str) or not result.query_id:
        raise MetastoreRecoveryError("recovery query ID is missing")
    return {
        "query_id": result.query_id,
        "row_count": row_count,
        "snapshot_id": snapshot_id,
    }


def _report_phase(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MetastoreRecoveryError(f"{name} evidence is missing")
    if not isinstance(value.get("query_id"), str) or not value["query_id"]:
        raise MetastoreRecoveryError(f"{name} query ID is missing")
    row_count = value.get("row_count")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count <= 0:
        raise MetastoreRecoveryError(f"{name} row count is invalid")
    snapshot_id = value.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not snapshot_id.isdigit():
        raise MetastoreRecoveryError(f"{name} snapshot ID is invalid")
    return value


def _topology(value: Any, *, metastore_running: bool) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MetastoreRecoveryError("service topology is missing")
    if value.get("metastore_running") is not metastore_running:
        raise MetastoreRecoveryError("Hive Metastore state does not match the drill phase")
    if value.get("database_running") is not True:
        raise MetastoreRecoveryError("metadata database stopped during the drill")
    for key in ("metastore_container_id", "database_container_id"):
        container_id = value.get(key)
        if (
            not isinstance(container_id, str)
            or len(container_id) != 64
            or any(character not in "0123456789abcdef" for character in container_id)
        ):
            raise MetastoreRecoveryError(f"{key} is invalid")
    return value
