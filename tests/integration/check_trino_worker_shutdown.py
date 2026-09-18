from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

SECURE_WORKERS = {
    "lakehouse-secure-worker-1": "trino-secure-worker",
    "lakehouse-secure-worker-2": "trino-secure-worker-2",
}
SECURITY = {
    "authentication": "password",
    "authorization": "ranger",
    "operator_user": "lakehouse-operator",
    "query_user": "platform_admin",
    "tls_verified": True,
    "transport": "https",
}


def validate(report: dict[str, Any]) -> list[str]:
    topology = report.get("topology", {})
    shutdown = report.get("shutdown", {})
    continuity = report.get("continuity", {})
    checks: dict[str, bool] = {
        "schema_version": report.get("schema_version") == "1.0",
        "status": report.get("status") == "succeeded",
        "active_nodes_before": topology.get("active_nodes_before") == 3,
        "active_workers_before": topology.get("active_workers_before") == 2,
        "active_nodes_after": topology.get("active_nodes_after") == 2,
        "active_workers_after": topology.get("active_workers_after") == 1,
        "target_node": shutdown.get("target_node_id") == "lakehouse-worker-2",
        "state_before": shutdown.get("state_before") == "ACTIVE",
        "state_after_request": shutdown.get("state_after_request") == "SHUTTING_DOWN",
        "target_unregistered": shutdown.get("target_registered_after") is False,
        "endpoint_stopped": shutdown.get("endpoint_stopped") is True,
        "grace_period": shutdown.get("grace_period_seconds") == 5,
        "silver_rows": continuity.get("silver_rows") == 2,
        "metadata_readable": continuity.get("snapshot_metadata_readable") is True,
    }
    security = report.get("security")
    if security is not None:
        target_node = shutdown.get("target_node_id")
        baseline = continuity.get("baseline", {})
        drained = continuity.get("drained", {})
        restored = continuity.get("restored", {})
        in_flight = report.get("in_flight_query", {})
        invariants = report.get("invariants", {})
        audit_window = report.get("audit_window", {})
        container_before = shutdown.get("container_id_before")
        container_restored = shutdown.get("container_id_restored")
        checks.update(
            security=security == SECURITY,
            audit_window=isinstance(audit_window.get("started_at"), str)
            and bool(audit_window.get("started_at"))
            and isinstance(audit_window.get("completed_at"), str)
            and bool(audit_window.get("completed_at")),
            target_node=target_node in SECURE_WORKERS,
            target_service=shutdown.get("target_service") == SECURE_WORKERS.get(target_node),
            container_before=_container_id(container_before),
            container_stopped=shutdown.get("container_running_after") is False,
            container_restored=_container_id(container_restored)
            and container_restored != container_before,
            restored_container_running=shutdown.get("container_running_restored") is True,
            active_nodes_restored=topology.get("active_nodes_restored") == 3,
            active_workers_restored=topology.get("active_workers_restored") == 2,
            target_restored=topology.get("target_registered_restored") is True,
            in_flight_query_id=isinstance(in_flight.get("query_id"), str)
            and bool(in_flight.get("query_id")),
            target_task_observed=in_flight.get("target_task_observed") is True,
            target_task_count=isinstance(in_flight.get("target_task_count"), int)
            and not isinstance(in_flight.get("target_task_count"), bool)
            and in_flight.get("target_task_count", 0) > 0,
            in_flight_finished=in_flight.get("terminal_state") == "FINISHED",
            fingerprint_complete=all(
                isinstance(phase.get("row_count"), int)
                and phase.get("row_count", 0) > 0
                and isinstance(phase.get("data_checksum"), str)
                and bool(phase.get("data_checksum"))
                and isinstance(phase.get("snapshot_id"), str)
                and phase.get("snapshot_id", "").isdigit()
                for phase in (baseline, drained, restored)
            ),
            fingerprint_preserved=(
                baseline.get("row_count"),
                baseline.get("data_checksum"),
                baseline.get("snapshot_id"),
            )
            == (
                drained.get("row_count"),
                drained.get("data_checksum"),
                drained.get("snapshot_id"),
            )
            == (
                restored.get("row_count"),
                restored.get("data_checksum"),
                restored.get("snapshot_id"),
            ),
            zero_query_interruption=invariants.get("zero_query_interruption") is True,
            data_preserved=invariants.get("data_preserved") is True,
            worker_capacity_restored=invariants.get("worker_capacity_restored") is True,
        )
    return [name for name, passed in checks.items() if not passed]


def _container_id(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def main() -> None:
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    failed = validate(report)
    if failed:
        raise SystemExit(f"Trino worker shutdown evidence failed: {', '.join(failed)}")
    print(
        json.dumps(
            {
                "status": "ready",
                "active_workers_before": 2,
                "active_workers_after": 1,
                "continuity_queries": 2,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
