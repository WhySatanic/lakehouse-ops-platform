from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

from lakehouse_ops.trino import TrinoClient

LONG_QUERY = """
SELECT left_value, right_value
FROM UNNEST(sequence(1, 10000)) AS left_side(left_value)
CROSS JOIN UNNEST(sequence(1, 10000)) AS right_side(right_value)
""".strip()
QUERY_ID = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass(frozen=True, slots=True)
class SubmittedQuery:
    query_id: str
    next_uri: str
    user: str


def coordinator_uri(server: str, next_uri: str) -> str:
    base = urlsplit(server.rstrip("/"))
    target = urlsplit(next_uri)
    if not target.path.startswith("/v1/statement/"):
        raise RuntimeError("Trino nextUri has an unexpected path")
    return urlunsplit((base.scheme, base.netloc, target.path, target.query, ""))


def submit_query(
    client: httpx.Client, server: str, *, user: str, sql: str
) -> SubmittedQuery:
    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "X-Trino-User": user,
        "X-Trino-Source": "resource-group-drill",
        "X-Trino-Time-Zone": "UTC",
    }
    response = client.post(
        f"{server.rstrip('/')}/v1/statement",
        content=sql,
        headers=headers,
    )
    response.raise_for_status()
    payload = response.json()
    query_id = payload.get("id")
    next_uri = payload.get("nextUri")
    if not isinstance(query_id, str) or not QUERY_ID.fullmatch(query_id):
        raise RuntimeError("Trino submission returned an invalid query ID")
    if not isinstance(next_uri, str):
        raise RuntimeError("Trino submission completed before queue evidence was captured")
    response = client.get(coordinator_uri(server, next_uri), headers=headers)
    response.raise_for_status()
    payload = response.json()
    if payload.get("id") != query_id:
        raise RuntimeError("Trino changed the query ID while advancing the protocol")
    next_uri = payload.get("nextUri")
    if not isinstance(next_uri, str):
        raise RuntimeError("Trino query completed before queue evidence was captured")
    return SubmittedQuery(query_id=query_id, next_uri=next_uri, user=user)


def cancel_query(
    client: httpx.Client, server: str, submitted: SubmittedQuery
) -> bool:
    response = client.delete(
        coordinator_uri(server, submitted.next_uri),
        headers={"X-Trino-User": submitted.user},
    )
    return response.status_code in {200, 204}


def runtime_snapshot(server: str, query_ids: list[str]) -> dict[str, dict[str, Any]]:
    quoted = ", ".join(f"'{query_id}'" for query_id in query_ids)
    with TrinoClient(server, user="lakehouse-bi-inspector", timeout=10.0) as client:
        rows = client.query(
            "SELECT query_id, state, resource_group_id "
            f"FROM system.runtime.queries WHERE query_id IN ({quoted})"
        )
    snapshot: dict[str, dict[str, Any]] = {}
    for row in rows:
        group = row.get("resource_group_id")
        if isinstance(group, list) and all(isinstance(part, str) for part in group):
            row["resource_group_id"] = ".".join(group)
        snapshot[row["query_id"]] = row
    return snapshot


def wait_for_queue(
    server: str, expected: dict[str, tuple[str, set[str]]]
) -> dict[str, dict[str, Any]]:
    deadline = time.monotonic() + 30
    last_snapshot: dict[str, dict[str, Any]] = {}
    while time.monotonic() < deadline:
        last_snapshot = runtime_snapshot(server, list(expected))
        if all(
            query_id in last_snapshot
            and last_snapshot[query_id].get("resource_group_id") == group
            and last_snapshot[query_id].get("state") in states
            for query_id, (group, states) in expected.items()
        ):
            return last_snapshot
        time.sleep(0.25)
    raise RuntimeError(f"resource group queue did not converge: {last_snapshot}")


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: exercise_trino_resource_groups.py SERVER REPORT")
    server = sys.argv[1].rstrip("/")
    report_path = Path(sys.argv[2])
    submitted: list[SubmittedQuery] = []
    cancellations: list[bool] = []
    snapshot: dict[str, dict[str, Any]]

    with httpx.Client(timeout=10.0) as client:
        try:
            ingestion = submit_query(
                client, server, user="lakehouse-ingestion-ci", sql=LONG_QUERY
            )
            submitted.append(ingestion)
            bi = submit_query(client, server, user="lakehouse-bi-ci", sql=LONG_QUERY)
            submitted.append(bi)
            adhoc_running = submit_query(
                client, server, user="lakehouse-analyst", sql=LONG_QUERY
            )
            submitted.append(adhoc_running)
            adhoc_queued = submit_query(
                client, server, user="lakehouse-analyst", sql="SELECT 42"
            )
            submitted.append(adhoc_queued)
            snapshot = wait_for_queue(
                server,
                {
                    ingestion.query_id: ("global.ingestion", {"RUNNING", "FINISHING"}),
                    bi.query_id: ("global.bi", {"RUNNING", "FINISHING"}),
                    adhoc_running.query_id: (
                        "global.adhoc",
                        {"RUNNING", "FINISHING"},
                    ),
                    adhoc_queued.query_id: ("global.adhoc", {"QUEUED"}),
                },
            )
        finally:
            cancellations = [
                cancel_query(client, server, query)
                for query in reversed(submitted)
            ]

    if len(cancellations) != 4 or not all(cancellations):
        raise RuntimeError(f"failed to cancel all drill queries: {cancellations}")
    with TrinoClient(server, user="lakehouse-bi-inspector", timeout=30.0) as client:
        silver_rows = client.query(
            "SELECT count(*) AS rows FROM lakehouse.silver.weather_hourly"
        )[0]["rows"]

    report = {
        "schema_version": "1.0",
        "status": "succeeded",
        "policy": {
            "root": "global",
            "root_hard_concurrency": 4,
            "groups": {
                "ingestion": {"hard_concurrency": 1, "max_queued": 4},
                "bi": {"hard_concurrency": 2, "max_queued": 10},
                "adhoc": {"hard_concurrency": 1, "max_queued": 3},
            },
        },
        "assignments": {
            "ingestion": snapshot[ingestion.query_id]["resource_group_id"],
            "bi": snapshot[bi.query_id]["resource_group_id"],
            "adhoc": snapshot[adhoc_running.query_id]["resource_group_id"],
        },
        "queue": {
            "group": snapshot[adhoc_queued.query_id]["resource_group_id"],
            "running_state": snapshot[adhoc_running.query_id]["state"],
            "queued_state": snapshot[adhoc_queued.query_id]["state"],
        },
        "cleanup": {"queries_submitted": 4, "queries_cancelled": 4},
        "continuity": {"silver_rows": silver_rows},
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = report_path.with_suffix(report_path.suffix + ".tmp")
    temporary_path.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")
    temporary_path.replace(report_path)
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
