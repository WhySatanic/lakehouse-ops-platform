from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from lakehouse_ops.trino import TrinoQueryResult

PHASES = ("baseline", "upgraded", "rolled_back", "restored")
EXPECTED_WORKLOAD = {
    "bronze_rows": 4,
    "silver_rows": 2,
    "reject_rows": 1,
    "duplicate_keys": 0,
    "latest_survivor": 1,
    "humidity_reject": 1,
}
WORKLOAD_SQL = """
SELECT
  (SELECT count(*) FROM lakehouse.bronze.weather_hourly) AS bronze_rows,
  (SELECT count(*) FROM lakehouse.silver.weather_hourly) AS silver_rows,
  (SELECT count(*) FROM lakehouse.silver.weather_hourly_rejects) AS reject_rows,
  (SELECT count(*) FROM (
    SELECT location_name, observed_at
    FROM lakehouse.silver.weather_hourly
    GROUP BY 1, 2
    HAVING count(*) > 1
  )) AS duplicate_keys,
  (SELECT count(*) FROM lakehouse.silver.weather_hourly
    WHERE location_name = 'moscow'
      AND observed_at = TIMESTAMP '2026-08-06 00:00:00'
      AND object_checksum = '0500bb2b50ec417801db2bce49ee65ed3f835ad6271792b3a3a083ecc44c572b'
      AND temperature_2m = 19.0) AS latest_survivor,
  (SELECT count(*) FROM lakehouse.silver.weather_hourly_rejects
    WHERE contains(quality_errors, 'humidity_out_of_range')) AS humidity_reject
""".strip()
FILES_SQL = """
SELECT
  count(*) AS data_file_count,
  coalesce(sum(record_count), 0) AS record_count,
  coalesce(sum(file_size_in_bytes), 0) AS total_size_bytes
FROM lakehouse.silver."weather_hourly$files"
WHERE content = 0
""".strip()
SNAPSHOT_TABLES = (
    ("bronze", "weather_hourly"),
    ("silver", "weather_hourly"),
    ("silver", "weather_hourly_rejects"),
)


class UpgradeRehearsalError(ValueError):
    pass


class QueryClient(Protocol):
    def query(self, sql: str) -> list[dict[str, Any]]: ...

    def query_with_stats(self, sql: str) -> TrinoQueryResult: ...


def load_upgrade_plan(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
        plan = json.loads(raw)
    except (OSError, json.JSONDecodeError) as error:
        raise UpgradeRehearsalError(f"cannot read upgrade plan: {path}") from error
    if not isinstance(plan, dict) or set(plan) != {
        "schema_version",
        "source",
        "target",
        "release_notes",
    }:
        raise UpgradeRehearsalError("upgrade plan has an unexpected shape")
    if plan.get("schema_version") != "1.0":
        raise UpgradeRehearsalError("upgrade plan schema must be 1.0")
    source = _version_spec(plan.get("source"), "source")
    target = _version_spec(plan.get("target"), "target")
    if int(target["version"]) != int(source["version"]) + 1:
        raise UpgradeRehearsalError("target must be the next Trino release")
    expected_notes = (
        f"https://trino.io/docs/current/release/release-{target['version']}.html"
    )
    if plan.get("release_notes") != expected_notes:
        raise UpgradeRehearsalError("release notes must match the target version")
    return {
        **plan,
        "source": source,
        "target": target,
        "plan_sha256": _digest(raw),
    }


def run_upgrade_rehearsal(
    client_factory: Callable[[], QueryClient],
    switch_cluster: Callable[[dict[str, str]], dict[str, Any]],
    initial_evidence: dict[str, Any],
    plan: dict[str, Any],
    *,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    source = _version_spec(plan.get("source"), "source")
    target = _version_spec(plan.get("target"), "target")
    phase_specs = (
        ("baseline", source),
        ("upgraded", target),
        ("rolled_back", source),
        ("restored", target),
    )
    records: list[dict[str, Any]] = []
    completed = False

    try:
        for index, (label, spec) in enumerate(phase_specs):
            transition = initial_evidence if index == 0 else switch_cluster(spec)
            client = client_factory()
            try:
                records.append(_capture_phase(client, label, spec, transition))
            finally:
                close = getattr(client, "close", None)
                if callable(close):
                    close()
        completed = True
    finally:
        if not completed:
            switch_cluster(target)

    now = clock or (lambda: datetime.now(UTC))
    report = {
        "schema_version": "1.0",
        "status": "ready",
        "experiment": "trino_version_upgrade_rehearsal",
        "collected_at": now().astimezone(UTC).isoformat(),
        "plan": {
            "sha256": plan.get("plan_sha256"),
            "source": source,
            "target": target,
            "release_notes": plan.get("release_notes"),
        },
        "phases": records,
        "compatibility": {
            "upgrade": "passed",
            "rollback": "passed",
            "target_restored": "passed",
        },
    }
    validate_upgrade_report(report, plan)
    return report


def validate_upgrade_report(report: dict[str, Any], plan: dict[str, Any]) -> None:
    source = _version_spec(plan.get("source"), "source")
    target = _version_spec(plan.get("target"), "target")
    expected_versions = (
        source["version"],
        target["version"],
        source["version"],
        target["version"],
    )
    if report.get("schema_version") != "1.0" or report.get("status") != "ready":
        raise UpgradeRehearsalError("upgrade report is not ready schema 1.0 evidence")
    if report.get("experiment") != "trino_version_upgrade_rehearsal":
        raise UpgradeRehearsalError("upgrade report has an unexpected experiment")
    report_plan = report.get("plan")
    if not isinstance(report_plan, dict):
        raise UpgradeRehearsalError("upgrade report has no plan evidence")
    if report_plan.get("sha256") != plan.get("plan_sha256"):
        raise UpgradeRehearsalError("upgrade report plan digest changed")
    if report_plan.get("source") != source or report_plan.get("target") != target:
        raise UpgradeRehearsalError("upgrade report versions changed")
    if report_plan.get("release_notes") != plan.get("release_notes"):
        raise UpgradeRehearsalError("upgrade report release notes changed")

    phases = report.get("phases")
    if not isinstance(phases, list) or len(phases) != len(PHASES):
        raise UpgradeRehearsalError("upgrade report must contain four phases")
    for phase, label, version in zip(phases, PHASES, expected_versions, strict=True):
        _validate_phase(phase, label, version)

    fingerprints = [phase["data_fingerprint"] for phase in phases]
    if len(set(fingerprints)) != 1:
        raise UpgradeRehearsalError("data compatibility changed across upgrade phases")
    node_ids = [{node["node_id"] for node in phase["nodes"]} for phase in phases]
    if any(value != node_ids[0] for value in node_ids[1:]):
        raise UpgradeRehearsalError("Trino node identities changed across the rehearsal")
    compatibility = report.get("compatibility")
    if compatibility != {
        "upgrade": "passed",
        "rollback": "passed",
        "target_restored": "passed",
    }:
        raise UpgradeRehearsalError("upgrade compatibility status is incomplete")


def _capture_phase(
    client: QueryClient,
    label: str,
    spec: dict[str, str],
    transition: dict[str, Any],
) -> dict[str, Any]:
    nodes = client.query(
        "SELECT node_id, node_version, coordinator, state "
        "FROM system.runtime.nodes ORDER BY node_id"
    )
    if len(nodes) != 3:
        raise UpgradeRehearsalError("upgrade phase did not restore three Trino nodes")
    normalized_nodes: list[dict[str, Any]] = []
    for node in nodes:
        normalized = {
            "node_id": node.get("node_id"),
            "node_version": node.get("node_version"),
            "coordinator": node.get("coordinator"),
            "state": node.get("state"),
        }
        if (
            not isinstance(normalized["node_id"], str)
            or normalized["node_version"] != spec["version"]
            or not isinstance(normalized["coordinator"], bool)
            or normalized["state"] != "active"
        ):
            raise UpgradeRehearsalError("Trino node membership does not match the phase")
        normalized_nodes.append(normalized)
    if sum(node["coordinator"] for node in normalized_nodes) != 1:
        raise UpgradeRehearsalError("upgrade phase must contain exactly one coordinator")

    workload_query = client.query_with_stats(WORKLOAD_SQL)
    workload = _single_row(workload_query.rows, "compatibility workload")
    if workload != EXPECTED_WORKLOAD:
        raise UpgradeRehearsalError("compatibility workload returned unexpected data")
    files = _single_row(client.query(FILES_SQL), "Iceberg files metadata")
    files = {
        "data_file_count": _positive_integer(files, "data_file_count"),
        "record_count": _positive_integer(files, "record_count"),
        "total_size_bytes": _positive_integer(files, "total_size_bytes"),
    }
    snapshots = {
        f"{schema}.{table}": _snapshot_id(client, schema, table)
        for schema, table in SNAPSHOT_TABLES
    }
    data_evidence = {"workload": workload, "files": files, "snapshots": snapshots}
    return {
        "phase": label,
        "expected_version": spec["version"],
        "image": spec["image"],
        "transition": transition,
        "nodes": normalized_nodes,
        "query_id": workload_query.query_id,
        **data_evidence,
        "data_fingerprint": _digest(json.dumps(data_evidence, sort_keys=True)),
    }


def _validate_phase(phase: Any, label: str, version: str) -> None:
    if not isinstance(phase, dict):
        raise UpgradeRehearsalError("upgrade phase must be an object")
    if phase.get("phase") != label or phase.get("expected_version") != version:
        raise UpgradeRehearsalError("upgrade phase order or version changed")
    if phase.get("image") != f"trinodb/trino:{version}":
        raise UpgradeRehearsalError("upgrade phase image does not match its version")
    transition = phase.get("transition")
    if (
        not isinstance(transition, dict)
        or transition.get("version") != version
        or transition.get("image") != phase["image"]
        or transition.get("active_nodes") != 3
        or not isinstance(transition.get("coordinator_id"), str)
        or not transition["coordinator_id"]
        or not isinstance(transition.get("container_image_id"), str)
        or not transition["container_image_id"].startswith("sha256:")
    ):
        raise UpgradeRehearsalError("upgrade transition evidence is incomplete")
    nodes = phase.get("nodes")
    if not isinstance(nodes, list) or len(nodes) != 3:
        raise UpgradeRehearsalError("upgrade phase node evidence is incomplete")
    if any(
        not isinstance(node, dict)
        or node.get("node_version") != version
        or node.get("state") != "active"
        for node in nodes
    ):
        raise UpgradeRehearsalError("upgrade phase node version is inconsistent")
    if phase.get("workload") != EXPECTED_WORKLOAD:
        raise UpgradeRehearsalError("upgrade phase workload evidence changed")
    files = phase.get("files")
    if not isinstance(files, dict) or any(
        isinstance(files.get(key), bool)
        or not isinstance(files.get(key), int)
        or files[key] <= 0
        for key in ("data_file_count", "record_count", "total_size_bytes")
    ):
        raise UpgradeRehearsalError("upgrade phase files evidence is invalid")
    snapshots = phase.get("snapshots")
    if not isinstance(snapshots, dict) or set(snapshots) != {
        f"{schema}.{table}" for schema, table in SNAPSHOT_TABLES
    }:
        raise UpgradeRehearsalError("upgrade phase snapshot evidence is incomplete")
    if any(not isinstance(value, str) or not value.isdigit() for value in snapshots.values()):
        raise UpgradeRehearsalError("upgrade phase snapshot ID is invalid")
    if not isinstance(phase.get("query_id"), str) or not phase["query_id"]:
        raise UpgradeRehearsalError("upgrade phase query ID is missing")
    data_evidence = {
        "workload": phase["workload"],
        "files": phase["files"],
        "snapshots": phase["snapshots"],
    }
    if phase.get("data_fingerprint") != _digest(
        json.dumps(data_evidence, sort_keys=True)
    ):
        raise UpgradeRehearsalError("upgrade phase data fingerprint is invalid")


def _version_spec(value: Any, name: str) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != {"version", "image"}:
        raise UpgradeRehearsalError(f"{name} version spec has an unexpected shape")
    version = value.get("version")
    image = value.get("image")
    if not isinstance(version, str) or not version.isdigit():
        raise UpgradeRehearsalError(f"{name} version must be numeric")
    if image != f"trinodb/trino:{version}":
        raise UpgradeRehearsalError(f"{name} image must match its version")
    return {"version": version, "image": image}


def _snapshot_id(client: QueryClient, schema: str, table: str) -> str:
    rows = client.query(
        f'SELECT CAST(snapshot_id AS varchar) AS snapshot_id FROM lakehouse.{schema}."'
        f'{table}$snapshots" ORDER BY committed_at DESC LIMIT 1'
    )
    row = _single_row(rows, f"{schema}.{table} snapshot")
    value = row.get("snapshot_id")
    if not isinstance(value, str) or not value.isdigit():
        raise UpgradeRehearsalError("snapshot ID must be numeric")
    return value


def _single_row(rows: Any, name: str) -> dict[str, Any]:
    if not isinstance(rows, (list, tuple)) or len(rows) != 1 or not isinstance(rows[0], dict):
        raise UpgradeRehearsalError(f"{name} returned an unexpected row count")
    return dict(rows[0])


def _positive_integer(row: dict[str, Any], key: str) -> int:
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise UpgradeRehearsalError(f"{key} must be a positive integer")
    return value


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
