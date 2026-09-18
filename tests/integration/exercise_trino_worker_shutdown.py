from __future__ import annotations

import argparse
import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from check_trino_worker_shutdown import validate
from exercise_trino_worker_recovery import (
    RecoveryProfile,
    cancel_query,
    compose_profile_args,
    container_state,
    data_state,
    drive_query,
    http_client,
    recovery_profile,
    restore_worker,
    submit_long_query,
    wait_for_target_task,
    wait_for_topology,
)


def worker_state(profile: RecoveryProfile, service: str) -> str:
    return subprocess.check_output(
        [
            "docker",
            "compose",
            *compose_profile_args(profile),
            "exec",
            "-T",
            "trino-secure-coordinator",
            "curl",
            "--fail",
            "--silent",
            "--show-error",
            "--header",
            f"X-Trino-User: {profile.operator_user}",
            f"http://{service}:8080/v1/info/state",
        ],
        text=True,
        timeout=15,
    ).strip().strip('"')


def request_shutdown(profile: RecoveryProfile, service: str) -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            *compose_profile_args(profile),
            "exec",
            "-T",
            "trino-secure-coordinator",
            "curl",
            "--fail",
            "--silent",
            "--show-error",
            "--request",
            "PUT",
            "--header",
            "Content-Type: application/json",
            "--header",
            f"X-Trino-User: {profile.operator_user}",
            "--data",
            '"SHUTTING_DOWN"',
            f"http://{service}:8080/v1/info/state",
        ],
        check=True,
        timeout=15,
    )


def wait_for_container_stop(service: str, profile: RecoveryProfile) -> dict[str, Any]:
    deadline = time.monotonic() + 60
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = container_state(service, profile)
        if last["running"] is False:
            return last
        time.sleep(0.5)
    raise RuntimeError(f"drained worker container did not stop: {last}")


def endpoint_stopped(service: str, profile: RecoveryProfile) -> bool:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            worker_state(profile, service)
        except subprocess.CalledProcessError:
            return True
        time.sleep(0.5)
    return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("server")
    parser.add_argument("report", type=Path)
    parser.add_argument(
        "--mode",
        choices=("authenticated-ranger",),
        default="authenticated-ranger",
    )
    parser.add_argument("--password", required=True)
    parser.add_argument("--ca-cert", type=Path, required=True)
    args = parser.parse_args()
    server = args.server.rstrip("/")
    profile = recovery_profile(args)
    if profile.security is None:
        raise RuntimeError("authenticated Ranger profile is required")

    audit_started_at = datetime.now(UTC)
    baseline_data = data_state(server, profile)
    restore_required = False
    next_uri = ""
    with (
        http_client(profile, profile.query_user, 15) as protocol_client,
        ThreadPoolExecutor(max_workers=1) as executor,
    ):
        future = None
        try:
            query_id, next_uri = submit_long_query(protocol_client, server, profile)
            future = executor.submit(drive_query, server, query_id, next_uri, profile)
            target_node_id, target_task_count = wait_for_target_task(
                server, query_id, profile
            )
            target_service = profile.worker_services[target_node_id]
            baseline_container = container_state(target_service, profile)
            if baseline_container["running"] is not True:
                raise RuntimeError("target worker is not running before the drain")
            if baseline_container["restart_policy"] != "on-failure":
                raise RuntimeError("target worker restart policy must be on-failure")
            baseline_topology = wait_for_topology(
                server,
                target_node_id,
                profile,
                active_nodes=3,
                active_workers=2,
                target_registered=True,
            )
            state_before = worker_state(profile, target_service)
            if state_before != "ACTIVE":
                raise RuntimeError(f"target worker state is not ACTIVE: {state_before}")

            restore_required = True
            request_shutdown(profile, target_service)
            state_after_request = worker_state(profile, target_service)
            if state_after_request != "SHUTTING_DOWN":
                raise RuntimeError(
                    "target worker did not enter SHUTTING_DOWN: "
                    f"{state_after_request}"
                )
            query_result = future.result(timeout=120)
            if query_result.get("terminal_state") != "FINISHED":
                raise RuntimeError(
                    "in-flight query did not finish during graceful drain: "
                    f"{query_result}"
                )
            drained_topology = wait_for_topology(
                server,
                target_node_id,
                profile,
                active_nodes=2,
                active_workers=1,
                target_registered=False,
            )
            stopped_container = wait_for_container_stop(target_service, profile)
            stopped_endpoint = endpoint_stopped(target_service, profile)
            if not stopped_endpoint:
                raise RuntimeError("target worker endpoint remained reachable after drain")
            drained_data = data_state(server, profile)
        finally:
            if next_uri and (future is None or not future.done()):
                cancel_query(protocol_client, server, next_uri, profile)
            if future is not None and not future.done():
                future.result(timeout=30)
            if restore_required:
                restore_worker(target_service, profile)

    restored_container = container_state(target_service, profile)
    restored_topology = wait_for_topology(
        server,
        target_node_id,
        profile,
        active_nodes=3,
        active_workers=2,
        target_registered=True,
    )
    restored_data = data_state(server, profile)
    fingerprints = {
        (
            phase["row_count"],
            phase["data_checksum"],
            phase["snapshot_id"],
        )
        for phase in (baseline_data, drained_data, restored_data)
    }
    report = {
        "schema_version": "1.0",
        "status": "succeeded",
        "security": profile.security,
        "audit_window": {
            "started_at": audit_started_at.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
        },
        "topology": {
            "active_nodes_before": baseline_topology["active_nodes"],
            "active_workers_before": baseline_topology["active_workers"],
            "active_nodes_after": drained_topology["active_nodes"],
            "active_workers_after": drained_topology["active_workers"],
            "active_nodes_restored": restored_topology["active_nodes"],
            "active_workers_restored": restored_topology["active_workers"],
            "target_registered_restored": restored_topology["target_registered"],
        },
        "shutdown": {
            "target_node_id": target_node_id,
            "target_service": target_service,
            "state_before": state_before,
            "state_after_request": state_after_request,
            "target_registered_after": drained_topology["target_registered"],
            "endpoint_stopped": stopped_endpoint,
            "grace_period_seconds": 5,
            "container_id_before": baseline_container["id"],
            "container_running_after": stopped_container["running"],
            "container_id_restored": restored_container["id"],
            "container_running_restored": restored_container["running"],
        },
        "in_flight_query": {
            "query_id": query_id,
            "target_task_observed": True,
            "target_task_count": target_task_count,
            **query_result,
        },
        "continuity": {
            "silver_rows": drained_data["row_count"],
            "snapshot_metadata_readable": True,
            "baseline": baseline_data,
            "drained": drained_data,
            "restored": restored_data,
        },
        "invariants": {
            "zero_query_interruption": query_result["terminal_state"] == "FINISHED",
            "data_preserved": len(fingerprints) == 1,
            "worker_capacity_restored": restored_topology["active_workers"] == 2,
        },
    }
    failed = validate(report)
    if failed:
        raise RuntimeError(f"generated shutdown evidence is invalid: {', '.join(failed)}")
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(args.report.read_text(encoding="utf-8").strip())


if __name__ == "__main__":
    main()
