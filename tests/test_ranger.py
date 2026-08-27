from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from lakehouse_ops.access_policy import load_access_policy
from lakehouse_ops.ranger import (
    MANAGED_DESCRIPTION,
    RangerAdminClient,
    RangerAdminError,
    compile_ranger_policies,
)

ROOT = Path(__file__).parents[1]
MODEL_PATH = ROOT / "config" / "access" / "role-policy.json"


def test_compile_ranger_policies_covers_roles_and_self_impersonation() -> None:
    policies = compile_ranger_policies(load_access_policy(MODEL_PATH), "lakehouse-trino")
    by_name = {policy["name"]: policy for policy in policies}

    admin = next(
        policy
        for policy in policies
        if policy["resources"].get("catalog", {}).get("values") == ["*"]
        and "column" in policy["resources"]
    )
    assert admin["policyItems"][0]["accesses"] == [
        {"type": "all", "isAllowed": True}
    ]
    analyst = next(
        policy
        for policy in policies
        if policy["resources"].get("schema", {}).get("values") == ["gold"]
    )
    assert analyst["resources"]["schema"]["values"] == ["gold"]
    assert analyst["policyItems"][0]["accesses"] == [
        {"type": "select", "isAllowed": True}
    ]
    self_policy = by_name["lakehouse-ops-self-impersonation"]
    assert self_policy["resources"]["trinouser"]["values"] == ["{USER}"]
    assert "data_engineer" in self_policy["policyItems"][0]["users"]
    sysinfo = by_name["lakehouse-ops-system-information"]
    metrics_item = next(
        item
        for item in sysinfo["policyItems"]
        if item["accesses"] == [{"type": "read_sysinfo", "isAllowed": True}]
    )
    assert "lakehouse-observer" in metrics_item["users"]
    assert all(policy["description"] == MANAGED_DESCRIPTION for policy in policies)
    assert all(policy["service"] == "lakehouse-trino" for policy in policies)


class RangerState:
    def __init__(self) -> None:
        self.service: dict[str, object] | None = None
        self.policies: list[dict[str, object]] = []
        self.users: set[str] = set()
        self.next_policy_id = 100
        self.inject_late_bootstrap = False

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "GET" and path.endswith("/service/name/lakehouse-trino"):
            if self.service is None:
                return httpx.Response(404, json={})
            return httpx.Response(200, json=self.service)
        if request.method == "POST" and path.endswith("/service"):
            self.service = {**_json(request), "id": 11}
            return httpx.Response(200, json=self.service)
        if request.method == "PUT" and "/service/11" in path:
            self.service = {**_json(request), "id": 11}
            return httpx.Response(200, json=self.service)
        if request.method == "GET" and path.endswith("/xusers/users"):
            return httpx.Response(
                200,
                json={"vXUsers": [{"name": user} for user in sorted(self.users)]},
            )
        if request.method == "POST" and path.endswith("/xusers/users/external"):
            self.users.add(str(_json(request)["name"]))
            return httpx.Response(204)
        if request.method == "GET" and path.endswith("/lakehouse-trino/policy"):
            return httpx.Response(200, json=self.policies)
        if request.method == "POST" and path.endswith("/policy"):
            if self.inject_late_bootstrap:
                self.inject_late_bootstrap = False
                self.policies.append(_bootstrap_policy(99))
                return httpx.Response(
                    400,
                    text="Another policy already exists for matching resource",
                )
            policy = {**_json(request), "id": self.next_policy_id}
            self.next_policy_id += 1
            self.policies.append(policy)
            return httpx.Response(200, json=policy)
        if request.method == "PUT" and "/policy/" in path:
            policy = _json(request)
            self.policies = [
                policy if current["id"] == policy["id"] else current
                for current in self.policies
            ]
            return httpx.Response(200, json=policy)
        if request.method == "DELETE" and "/policy/" in path:
            policy_id = int(path.rsplit("/", 1)[1])
            self.policies = [policy for policy in self.policies if policy["id"] != policy_id]
            return httpx.Response(204)
        return httpx.Response(500, text=f"unexpected {request.method} {path}")


def test_sync_creates_then_leaves_service_and_policies_unchanged() -> None:
    state = RangerState()
    transport = httpx.MockTransport(state.handle)

    with RangerAdminClient(
        "http://ranger.test", "admin", "secret", transport=transport
    ) as client:
        created = client.sync(
            model_path=MODEL_PATH,
            service_name="lakehouse-trino",
            trino_jdbc_url="jdbc:trino://trino-coordinator:8080",
            service_user="platform_admin",
        )
        unchanged = client.sync(
            model_path=MODEL_PATH,
            service_name="lakehouse-trino",
            trino_jdbc_url="jdbc:trino://trino-coordinator:8080",
            service_user="platform_admin",
        )

    assert created["service_status"] == "created"
    assert created["users"]["created"] == len(state.users)
    assert created["policies"]["created"] == len(state.policies)
    assert unchanged["service_status"] == "unchanged"
    assert unchanged["users"]["created"] == 0
    assert unchanged["policies"] == {
        "desired": len(state.policies),
        "bootstrap_deleted": 0,
        "created": 0,
        "updated": 0,
        "unchanged": len(state.policies),
        "deleted": 0,
    }


def test_sync_applies_active_break_glass_lease(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    lease_path = tmp_path / "break-glass.json"
    lease_path.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "grant_id": "BG-TEST-1",
                "user": "incident-responder",
                "role": "platform_admin",
                "approved_by": "incident-commander",
                "ticket": "INC-TEST-1",
                "reason": "recover query access",
                "issued_at": (now - timedelta(minutes=5)).isoformat(),
                "expires_at": (now + timedelta(minutes=30)).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    state = RangerState()

    with RangerAdminClient(
        "http://ranger.test",
        "admin",
        "secret",
        transport=httpx.MockTransport(state.handle),
    ) as client:
        report = client.sync(
            model_path=MODEL_PATH,
            service_name="lakehouse-trino",
            trino_jdbc_url="jdbc:trino://trino-coordinator:8080",
            service_user="platform_admin",
            break_glass_path=lease_path,
        )

    assert report["break_glass"]["status"] == "active"
    assert "incident-responder" in state.users
    admin_policies = [
        policy
        for policy in state.policies
        if any(
            "incident-responder" in item["users"]
            for item in policy.get("policyItems", [])
        )
    ]
    assert admin_policies


def test_sync_updates_drift_and_deletes_stale_managed_policy() -> None:
    state = RangerState()
    state.service = {
        "id": 11,
        "name": "lakehouse-trino",
        "type": "trino",
        "isEnabled": False,
        "configs": {},
    }
    desired = compile_ranger_policies(load_access_policy(MODEL_PATH), "lakehouse-trino")
    state.policies = [
        {**desired[0], "id": 50, "resources": {}},
        {
            **desired[1],
            "id": 51,
            "name": "lakehouse-ops-stale",
            "description": MANAGED_DESCRIPTION,
            "resources": {"catalog": {"values": ["stale"]}},
        },
        _bootstrap_policy(52),
    ]
    transport = httpx.MockTransport(state.handle)

    with RangerAdminClient(
        "http://ranger.test", "admin", "secret", transport=transport
    ) as client:
        report = client.sync(
            model_path=MODEL_PATH,
            service_name="lakehouse-trino",
            trino_jdbc_url="jdbc:trino://trino-coordinator:8080",
            service_user="platform_admin",
        )

    assert report["service_status"] == "updated"
    assert report["policies"]["updated"] == 1
    assert report["policies"]["deleted"] == 1
    assert report["policies"]["bootstrap_deleted"] == 1
    assert "lakehouse-ops-stale" not in {policy["name"] for policy in state.policies}


def test_sync_removes_bootstrap_policy_created_during_first_policy_write() -> None:
    state = RangerState()
    state.inject_late_bootstrap = True

    with RangerAdminClient(
        "http://ranger.test",
        "admin",
        "secret",
        transport=httpx.MockTransport(state.handle),
    ) as client:
        report = client.sync(
            model_path=MODEL_PATH,
            service_name="lakehouse-trino",
            trino_jdbc_url="jdbc:trino://trino-coordinator:8080",
            service_user="platform_admin",
        )

    assert report["policies"]["bootstrap_deleted"] == 1
    assert all(not policy["name"].startswith("all - ") for policy in state.policies)


def test_client_reports_ranger_http_failure() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(500, text="boom"))

    with RangerAdminClient(
        "http://ranger.test", "admin", "secret", transport=transport
    ) as client, pytest.raises(RangerAdminError, match="failed with 500: boom"):
        client.sync(
            model_path=MODEL_PATH,
            service_name="lakehouse-trino",
            trino_jdbc_url="jdbc:trino://trino-coordinator:8080",
            service_user="platform_admin",
        )


def test_client_reports_ranger_network_failure() -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    with RangerAdminClient(
        "http://ranger.test", "admin", "secret", transport=httpx.MockTransport(fail)
    ) as client, pytest.raises(RangerAdminError, match="connection refused"):
        client.sync(
            model_path=MODEL_PATH,
            service_name="lakehouse-trino",
            trino_jdbc_url="jdbc:trino://trino-coordinator:8080",
            service_user="platform_admin",
        )


def _json(request: httpx.Request) -> dict[str, object]:
    return json.loads(request.content)


def _bootstrap_policy(policy_id: int) -> dict[str, object]:
    return {
        "id": policy_id,
        "service": "lakehouse-trino",
        "name": "all - catalog",
        "description": "Policy for all - catalog",
        "policyItems": [
            {"users": ["platform_admin"], "delegateAdmin": True, "accesses": []}
        ],
    }
