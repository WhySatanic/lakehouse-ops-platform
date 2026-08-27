from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Protocol

from lakehouse_ops.trino import TrinoQueryResult

INCIDENT = "trino_worker_abrupt_loss"
TABLE = "lakehouse.silver.weather_hourly"
WORKER_SERVICES = {
    "lakehouse-worker-1": "trino-worker",
    "lakehouse-worker-2": "trino-worker-2",
}
REMOTE_TASK_MAX_ERROR_DURATION = "15s"
STATE_SQL = f"""
SELECT
  count(*) AS row_count,
  lower(to_hex(checksum(ROW(
    object_checksum,
    source,
    location_name,
    latitude,
    longitude,
    observed_at,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    wind_speed_10m
  )))) AS data_checksum,
  (SELECT CAST(snapshot_id AS varchar)
   FROM lakehouse.silver."weather_hourly$snapshots"
   ORDER BY committed_at DESC LIMIT 1) AS snapshot_id
FROM {TABLE}
""".strip()
HEX_DIGEST = re.compile(r"^[0-9a-f]+$")


class TrinoWorkerRecoveryError(ValueError):
    pass


class QueryClient(Protocol):
    def query_with_stats(self, sql: str) -> TrinoQueryResult: ...


def capture_data_state(client: QueryClient) -> dict[str, Any]:
    result = client.query_with_stats(STATE_SQL)
    if len(result.rows) != 1:
        raise TrinoWorkerRecoveryError("data query returned an unexpected row count")
    state = {
        "query_id": result.query_id,
        "row_count": result.rows[0].get("row_count"),
        "data_checksum": result.rows[0].get("data_checksum"),
        "snapshot_id": result.rows[0].get("snapshot_id"),
    }
    _data_state(state, "captured")
    return state


def validate_trino_worker_recovery_report(report: dict[str, Any]) -> None:
    if report.get("schema_version") != "1.0" or report.get("status") != "recovered":
        raise TrinoWorkerRecoveryError("report is not recovered schema 1.0 evidence")
    if report.get("incident") != INCIDENT or report.get("table") != TABLE:
        raise TrinoWorkerRecoveryError("report incident or table is unexpected")
    if report.get("policy") != {
        "retry_policy": "NONE",
        "remote_task_max_error_duration": REMOTE_TASK_MAX_ERROR_DURATION,
    }:
        raise TrinoWorkerRecoveryError("worker recovery policy is unexpected")
    if not isinstance(report.get("collected_at"), str) or not report["collected_at"]:
        raise TrinoWorkerRecoveryError("report collection time is missing")
    duration = report.get("worker_loss_duration_seconds")
    if isinstance(duration, bool) or not isinstance(duration, (int, float)) or duration < 0:
        raise TrinoWorkerRecoveryError("worker loss duration is invalid")

    baseline = _data_phase(report.get("baseline"), "baseline", 3, 2, True)
    degraded = _data_phase(
        report.get("degraded_recovery"), "degraded recovery", 2, 1, False
    )
    restored = _data_phase(report.get("restored"), "restored", 3, 2, True)
    in_flight = report.get("in_flight_query")
    if not isinstance(in_flight, dict):
        raise TrinoWorkerRecoveryError("in-flight query evidence is missing")
    if not _non_empty(in_flight.get("query_id")):
        raise TrinoWorkerRecoveryError("in-flight query ID is missing")
    if in_flight.get("target_task_observed") is not True:
        raise TrinoWorkerRecoveryError("target worker task was not observed")
    task_count = in_flight.get("target_task_count")
    if isinstance(task_count, bool) or not isinstance(task_count, int) or task_count < 1:
        raise TrinoWorkerRecoveryError("target worker task count is invalid")
    if in_flight.get("terminal_state") != "FAILED":
        raise TrinoWorkerRecoveryError("in-flight query did not fail after worker loss")
    if in_flight.get("error_name") != "TOO_MANY_REQUESTS_FAILED":
        raise TrinoWorkerRecoveryError("in-flight error is not a remote-task failure")
    if not _non_empty(in_flight.get("failure_message")):
        raise TrinoWorkerRecoveryError("in-flight failure_message is missing")

    loss = report.get("loss")
    if not isinstance(loss, dict):
        raise TrinoWorkerRecoveryError("worker loss evidence is missing")
    target_node_id = loss.get("target_node_id")
    if target_node_id not in WORKER_SERVICES:
        raise TrinoWorkerRecoveryError("worker loss target node is unexpected")
    if loss.get("target_service") != WORKER_SERVICES[target_node_id]:
        raise TrinoWorkerRecoveryError("worker loss target service is unexpected")
    if loss.get("signal") != "SIGKILL":
        raise TrinoWorkerRecoveryError("worker loss signal is not SIGKILL")
    _container_id(loss.get("container_id_before"), "lost worker container")
    if loss.get("restart_policy_before") != "on-failure":
        raise TrinoWorkerRecoveryError("worker restart policy before loss is unexpected")
    if loss.get("container_running_after") is not False:
        raise TrinoWorkerRecoveryError("worker container remained running after SIGKILL")
    _topology(loss.get("topology"), "loss", 2, 1, False)

    restored_container = restored.get("container")
    if not isinstance(restored_container, dict):
        raise TrinoWorkerRecoveryError("restored worker container evidence is missing")
    restored_id = _container_id(restored_container.get("id"), "restored worker container")
    if restored_id == loss["container_id_before"]:
        raise TrinoWorkerRecoveryError("worker container was not recreated")
    if restored_container.get("running") is not True:
        raise TrinoWorkerRecoveryError("restored worker container is not running")
    if restored_container.get("restart_policy") != "on-failure":
        raise TrinoWorkerRecoveryError("restored worker restart policy is unexpected")

    fingerprints = {
        (phase["row_count"], phase["data_checksum"], phase["snapshot_id"])
        for phase in (baseline, degraded, restored)
    }
    if len(fingerprints) != 1:
        raise TrinoWorkerRecoveryError("Iceberg data changed during worker recovery")
    expected_invariants = {
        "in_flight_failure_observed": True,
        "degraded_retry_succeeded": True,
        "row_count_preserved": True,
        "data_checksum_preserved": True,
        "snapshot_preserved": True,
        "worker_capacity_restored": True,
        "restart_policy_restored": True,
    }
    if report.get("invariants") != expected_invariants:
        raise TrinoWorkerRecoveryError("worker recovery invariants are incomplete")


def write_trino_worker_recovery_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _data_phase(
    value: Any,
    name: str,
    active_nodes: int,
    active_workers: int,
    target_registered: bool,
) -> dict[str, Any]:
    state = _data_state(value, name)
    _topology(
        state.get("topology"),
        name,
        active_nodes,
        active_workers,
        target_registered,
    )
    return state


def _data_state(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TrinoWorkerRecoveryError(f"{name} data evidence is missing")
    if not _non_empty(value.get("query_id")):
        raise TrinoWorkerRecoveryError(f"{name} query ID is missing")
    row_count = value.get("row_count")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count <= 0:
        raise TrinoWorkerRecoveryError(f"{name} row count is invalid")
    checksum = value.get("data_checksum")
    if not isinstance(checksum, str) or not HEX_DIGEST.fullmatch(checksum):
        raise TrinoWorkerRecoveryError(f"{name} data checksum is invalid")
    snapshot_id = value.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not snapshot_id.isdigit():
        raise TrinoWorkerRecoveryError(f"{name} snapshot ID is invalid")
    return value


def _topology(
    value: Any,
    name: str,
    active_nodes: int,
    active_workers: int,
    target_registered: bool,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TrinoWorkerRecoveryError(f"{name} topology evidence is missing")
    if value.get("active_nodes") != active_nodes:
        raise TrinoWorkerRecoveryError(f"{name} active node count is unexpected")
    if value.get("active_workers") != active_workers:
        raise TrinoWorkerRecoveryError(f"{name} active worker count is unexpected")
    if value.get("target_registered") is not target_registered:
        raise TrinoWorkerRecoveryError(f"{name} target registration is unexpected")
    return value


def _container_id(value: Any, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise TrinoWorkerRecoveryError(f"{name} ID is invalid")
    return value


def _non_empty(value: Any) -> bool:
    return isinstance(value, str) and bool(value)
