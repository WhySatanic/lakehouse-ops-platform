from __future__ import annotations

import argparse
import json
import re
import ssl
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

from lakehouse_ops.trino import TrinoClient
from lakehouse_ops.trino_worker_recovery import (
    REMOTE_TASK_MAX_ERROR_DURATION,
    SECURE_WORKER_SERVICES,
    WORKER_SERVICES,
    capture_data_state,
    validate_trino_worker_recovery_report,
    write_trino_worker_recovery_report,
)

LONG_QUERY = """
SELECT count(*)
FROM UNNEST(sequence(1, 10000)) AS left_side(left_value)
CROSS JOIN UNNEST(sequence(1, 10000)) AS right_side(right_value)
WHERE sin(CAST(left_value AS double)) + cos(CAST(right_value AS double)) > -3
""".strip()
QUERY_ID = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass(frozen=True, slots=True)
class RecoveryProfile:
    worker_services: dict[str, str]
    compose_profiles: tuple[str, ...]
    query_user: str
    operator_user: str
    password: str | None = None
    verify: ssl.SSLContext | bool = True
    security: dict[str, Any] | None = None


def headers(user: str) -> dict[str, str]:
    return {
        "X-Trino-User": user,
        "X-Trino-Source": "abrupt-worker-recovery-drill",
        "X-Trino-Time-Zone": "UTC",
    }


def http_client(profile: RecoveryProfile, user: str, timeout: float) -> httpx.Client:
    auth = (user, profile.password) if profile.password is not None else None
    return httpx.Client(
        auth=auth,
        timeout=timeout,
        verify=profile.verify,
    )


def coordinator_uri(server: str, uri: str, *, prefix: str) -> str:
    base = urlsplit(server.rstrip("/"))
    target = urlsplit(uri)
    if not target.path.startswith(prefix):
        raise RuntimeError(f"Trino URI has an unexpected path: {target.path}")
    return urlunsplit((base.scheme, base.netloc, target.path, target.query, ""))


def submit_long_query(
    client: httpx.Client, server: str, profile: RecoveryProfile
) -> tuple[str, str]:
    request_headers = headers(profile.query_user)
    response = client.post(
        f"{server.rstrip('/')}/v1/statement",
        content=LONG_QUERY,
        headers={**request_headers, "Content-Type": "text/plain; charset=utf-8"},
    )
    response.raise_for_status()
    payload = response.json()
    query_id = payload.get("id")
    next_uri = payload.get("nextUri")
    if not isinstance(query_id, str) or not QUERY_ID.fullmatch(query_id):
        raise RuntimeError("Trino submission returned an invalid query ID")
    if not isinstance(next_uri, str):
        raise RuntimeError("long query completed before worker task evidence was captured")
    response = client.get(
        coordinator_uri(server, next_uri, prefix="/v1/statement/"),
        headers=request_headers,
    )
    response.raise_for_status()
    payload = response.json()
    next_uri = payload.get("nextUri")
    if payload.get("id") != query_id or not isinstance(next_uri, str):
        raise RuntimeError("long query completed before worker task evidence was captured")
    return query_id, next_uri


def wait_for_target_task(
    server: str, query_id: str, profile: RecoveryProfile
) -> tuple[str, int]:
    deadline = time.monotonic() + 30
    last_rows: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        with http_client(profile, profile.operator_user, 10) as transport, TrinoClient(
            server, user=profile.operator_user, client=transport
        ) as client:
            last_rows = client.query(
                "SELECT node_id, count(*) AS task_count FROM system.runtime.tasks "
                f"WHERE query_id = '{query_id}' AND state = 'RUNNING' "
                "GROUP BY node_id ORDER BY node_id"
            )
        for row in last_rows:
            node_id = row.get("node_id")
            task_count = row.get("task_count")
            if (
                node_id in profile.worker_services
                and isinstance(task_count, int)
                and task_count > 0
            ):
                return node_id, task_count
        time.sleep(0.25)
    raise RuntimeError(f"no worker task was observed for query {query_id}: {last_rows}")


def drive_query(
    server: str, query_id: str, next_uri: str, profile: RecoveryProfile
) -> dict[str, str]:
    request_headers = headers(profile.query_user)
    with http_client(profile, profile.query_user, 15) as client:
        for _ in range(1000):
            response = client.get(
                coordinator_uri(server, next_uri, prefix="/v1/statement/"),
                headers=request_headers,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("id") != query_id:
                raise RuntimeError("Trino changed the query ID while advancing the protocol")
            if error := payload.get("error"):
                return {
                    "terminal_state": "FAILED",
                    "error_name": str(error.get("errorName") or "TRINO_QUERY_ERROR"),
                    "failure_message": str(
                        error.get("message") or "query failed without a message"
                    ),
                }
            next_value = payload.get("nextUri")
            if not next_value:
                state = str((payload.get("stats") or {}).get("state") or "FINISHED")
                return {"terminal_state": state}
            if not isinstance(next_value, str):
                raise RuntimeError("Trino nextUri is not a string")
            next_uri = next_value
    raise RuntimeError("in-flight query exceeded the protocol page safety limit")


def cancel_query(
    client: httpx.Client,
    server: str,
    next_uri: str,
    profile: RecoveryProfile,
) -> None:
    with suppress(httpx.HTTPError):
        client.delete(
            coordinator_uri(server, next_uri, prefix="/v1/statement/"),
            headers=headers(profile.query_user),
        )


def topology(
    server: str, target_node_id: str, profile: RecoveryProfile
) -> dict[str, Any]:
    with http_client(profile, profile.operator_user, 15) as transport, TrinoClient(
        server, user=profile.operator_user, client=transport
    ) as client:
        row = client.query(
            "SELECT count(*) AS active_nodes, "
            "count_if(coordinator = false) AS active_workers, "
            f"count_if(node_id = '{target_node_id}') > 0 AS target_registered "
            "FROM system.runtime.nodes WHERE state = 'active'"
        )[0]
    return row


def wait_for_topology(
    server: str,
    target_node_id: str,
    profile: RecoveryProfile,
    *,
    active_nodes: int,
    active_workers: int,
    target_registered: bool,
) -> dict[str, Any]:
    deadline = time.monotonic() + 60
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = topology(server, target_node_id, profile)
        if last == {
            "active_nodes": active_nodes,
            "active_workers": active_workers,
            "target_registered": target_registered,
        }:
            return last
        time.sleep(0.5)
    raise RuntimeError(f"Trino topology did not converge: {last}")


def compose_profile_args(profile: RecoveryProfile) -> list[str]:
    return [
        option
        for name in profile.compose_profiles
        for option in ("--profile", name)
    ]


def container_state(service: str, profile: RecoveryProfile) -> dict[str, Any]:
    container_id = subprocess.check_output(
        [
            "docker",
            "compose",
            *compose_profile_args(profile),
            "ps",
            "--all",
            "-q",
            service,
        ],
        text=True,
    ).strip()
    if not container_id:
        raise RuntimeError(f"Compose container not found: {service}")
    raw = subprocess.check_output(
        [
            "docker",
            "inspect",
            "--format",
            "{{json .State}}|{{json .HostConfig.RestartPolicy.Name}}",
            container_id,
        ],
        text=True,
    ).strip()
    state_raw, restart_raw = raw.split("|", maxsplit=1)
    state = json.loads(state_raw)
    return {
        "id": container_id,
        "running": state.get("Running") is True,
        "restart_policy": json.loads(restart_raw),
    }


def kill_worker(container_id: str) -> None:
    subprocess.run(
        ["docker", "update", "--restart=no", container_id],
        check=True,
        timeout=30,
    )
    subprocess.run(
        ["docker", "kill", "--signal=KILL", container_id],
        check=True,
        timeout=30,
    )


def restore_worker(service: str, profile: RecoveryProfile) -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            *compose_profile_args(profile),
            "up",
            "-d",
            "--wait",
            "--no-deps",
            "--force-recreate",
            service,
        ],
        check=True,
        timeout=180,
    )


def data_state(server: str, profile: RecoveryProfile) -> dict[str, Any]:
    with http_client(profile, profile.query_user, 45) as transport, TrinoClient(
        server, user=profile.query_user, client=transport
    ) as client:
        return capture_data_state(client)


def recovery_profile(args: argparse.Namespace) -> RecoveryProfile:
    if args.mode == "default":
        return RecoveryProfile(
            worker_services=WORKER_SERVICES,
            compose_profiles=("query",),
            query_user="lakehouse-bi-recovery",
            operator_user="lakehouse-operator",
        )
    if not args.password:
        raise ValueError("--password is required for authenticated Ranger recovery")
    if args.ca_cert is None or not args.ca_cert.is_file():
        raise ValueError("--ca-cert must name the trusted Trino certificate")
    if urlsplit(args.server).scheme != "https":
        raise ValueError("authenticated Ranger recovery requires an HTTPS server")
    return RecoveryProfile(
        worker_services=SECURE_WORKER_SERVICES,
        compose_profiles=("security", "catalog", "secure-query"),
        query_user="platform_admin",
        operator_user="lakehouse-operator",
        password=args.password,
        verify=ssl.create_default_context(cafile=str(args.ca_cert)),
        security={
            "authentication": "password",
            "authorization": "ranger",
            "operator_user": "lakehouse-operator",
            "query_user": "platform_admin",
            "tls_verified": True,
            "transport": "https",
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("server")
    parser.add_argument("report", type=Path)
    parser.add_argument(
        "--mode",
        choices=("default", "authenticated-ranger"),
        default="default",
    )
    parser.add_argument("--password")
    parser.add_argument("--ca-cert", type=Path)
    args = parser.parse_args()
    server = args.server.rstrip("/")
    profile = recovery_profile(args)

    baseline_data = data_state(server, profile)

    restore_required = False
    next_uri = ""
    loss_started = 0.0
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
                raise RuntimeError("target worker is not running before the drill")
            if baseline_container["restart_policy"] != "on-failure":
                raise RuntimeError("target worker restart policy must be on-failure")
            baseline = {
                **baseline_data,
                "topology": wait_for_topology(
                    server,
                    target_node_id,
                    profile,
                    active_nodes=3,
                    active_workers=2,
                    target_registered=True,
                ),
            }
            restore_required = True
            loss_started = time.monotonic()
            kill_worker(baseline_container["id"])
            failed = future.result(timeout=45)
            if failed.get("terminal_state") != "FAILED":
                raise RuntimeError(
                    "in-flight query reached "
                    f"{failed.get('terminal_state')} instead of failing after SIGKILL"
                )
            loss_topology = wait_for_topology(
                server,
                target_node_id,
                profile,
                active_nodes=2,
                active_workers=1,
                target_registered=False,
            )
            stopped_container = container_state(target_service, profile)
            if stopped_container["running"] is not False:
                raise RuntimeError("target worker container remained running after SIGKILL")
            degraded = {
                **data_state(server, profile),
                "topology": loss_topology,
            }
        finally:
            if next_uri and (future is None or not future.done()):
                cancel_query(protocol_client, server, next_uri, profile)
            if future is not None and not future.done():
                future.result(timeout=30)
            if restore_required:
                restore_worker(target_service, profile)

    restored_container = container_state(target_service, profile)
    restored = {
        **data_state(server, profile),
        "topology": wait_for_topology(
            server,
            target_node_id,
            profile,
            active_nodes=3,
            active_workers=2,
            target_registered=True,
        ),
        "container": restored_container,
    }
    report = {
        "schema_version": "1.0",
        "status": "recovered",
        "incident": "trino_worker_abrupt_loss",
        "table": "lakehouse.silver.weather_hourly",
        "policy": {
            "retry_policy": "NONE",
            "remote_task_max_error_duration": REMOTE_TASK_MAX_ERROR_DURATION,
        },
        "collected_at": datetime.now(UTC).isoformat(),
        "worker_loss_duration_seconds": round(time.monotonic() - loss_started, 3),
        "baseline": baseline,
        "in_flight_query": {
            "query_id": query_id,
            "target_task_observed": True,
            "target_task_count": target_task_count,
            **failed,
        },
        "loss": {
            "target_service": target_service,
            "target_node_id": target_node_id,
            "signal": "SIGKILL",
            "container_id_before": baseline_container["id"],
            "restart_policy_before": baseline_container["restart_policy"],
            "container_running_after": stopped_container["running"],
            "topology": loss_topology,
        },
        "degraded_recovery": degraded,
        "restored": restored,
        "invariants": {
            "in_flight_failure_observed": True,
            "degraded_retry_succeeded": True,
            "row_count_preserved": baseline["row_count"]
            == degraded["row_count"]
            == restored["row_count"],
            "data_checksum_preserved": baseline["data_checksum"]
            == degraded["data_checksum"]
            == restored["data_checksum"],
            "snapshot_preserved": baseline["snapshot_id"]
            == degraded["snapshot_id"]
            == restored["snapshot_id"],
            "worker_capacity_restored": restored["topology"]["active_workers"] == 2,
            "restart_policy_restored": restored_container["restart_policy"]
            == "on-failure",
        },
    }
    if profile.security is not None:
        report["security"] = profile.security
    validate_trino_worker_recovery_report(report)
    write_trino_worker_recovery_report(args.report, report)
    print(args.report.read_text(encoding="utf-8").strip())


if __name__ == "__main__":
    main()
