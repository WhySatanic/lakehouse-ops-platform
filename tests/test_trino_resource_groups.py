from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import httpx

CHECKER_PATH = Path(__file__).parent / "integration" / "check_trino_resource_groups.py"
SPEC = importlib.util.spec_from_file_location("check_trino_resource_groups", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)

EXERCISE_PATH = (
    Path(__file__).parent / "integration" / "exercise_trino_resource_groups.py"
)
EXERCISE_SPEC = importlib.util.spec_from_file_location(
    "exercise_trino_resource_groups", EXERCISE_PATH
)
assert EXERCISE_SPEC is not None and EXERCISE_SPEC.loader is not None
EXERCISE = importlib.util.module_from_spec(EXERCISE_SPEC)
sys.modules[EXERCISE_SPEC.name] = EXERCISE
EXERCISE_SPEC.loader.exec_module(EXERCISE)


def valid_report() -> dict[str, object]:
    return {
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
            "ingestion": "global.ingestion",
            "bi": "global.bi",
            "adhoc": "global.adhoc",
        },
        "queue": {
            "group": "global.adhoc",
            "running_state": "RUNNING",
            "queued_state": "QUEUED",
        },
        "cleanup": {"queries_submitted": 4, "queries_cancelled": 4},
        "continuity": {"silver_rows": 2},
    }


def test_validate_accepts_resource_group_queue_evidence() -> None:
    assert CHECKER.validate(valid_report()) == []


def test_validate_rejects_wrong_assignment_and_missing_queue() -> None:
    report = valid_report()
    report["assignments"]["adhoc"] = "global.bi"
    report["queue"]["queued_state"] = "RUNNING"

    assert CHECKER.validate(report) == ["assignments", "queued_state"]


def test_submit_query_advances_trino_protocol_before_returning() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        page = len(requests)
        return httpx.Response(
            200,
            json={
                "id": "20260825_000000_00000_test",
                "nextUri": (
                    "http://trino-coordinator:8080/v1/statement/"
                    f"20260825_000000_00000_test/{page}"
                ),
                "stats": {"state": "QUEUED"},
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        submitted = EXERCISE.submit_query(
            client,
            "http://localhost:8080",
            user="lakehouse-bi-ci",
            sql="SELECT 1",
        )

    assert [request.method for request in requests] == ["POST", "GET"]
    assert str(requests[1].url) == (
        "http://localhost:8080/v1/statement/20260825_000000_00000_test/1"
    )
    assert submitted.next_uri.endswith("/20260825_000000_00000_test/2")
