from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import httpx

from lakehouse_ops.access_policy import load_access_policy
from lakehouse_ops.break_glass import apply_break_glass_lease

MANAGED_DESCRIPTION = "Managed by Lakehouse Ops role-policy schema 1.0"


class RangerAdminError(RuntimeError):
    pass


def compile_ranger_policies(
    model: dict[str, Any], service_name: str
) -> list[dict[str, Any]]:
    users_by_role = _users_by_role(model)
    all_users = sorted({user for users in users_by_role.values() for user in users})
    policies: list[dict[str, Any]] = []
    for role_name, permissions in model["roles"].items():
        users = users_by_role.get(role_name, [])
        if not users:
            continue
        for index, grant in enumerate(permissions.get("catalogs", []), start=1):
            access = "all" if grant["access"] == "all" else "select"
            policies.append(
                _policy(
                    service_name,
                    f"lakehouse-ops-{role_name}-catalog-{index}",
                    {"catalog": _resource(grant["catalogs"])},
                    users,
                    [access],
                )
            )
        for index, grant in enumerate(permissions.get("schemas", []), start=1):
            policies.append(
                _policy(
                    service_name,
                    f"lakehouse-ops-{role_name}-schema-{index}",
                    {
                        "catalog": _resource(grant["catalogs"]),
                        "schema": _resource(grant["schemas"]),
                    },
                    users,
                    ["all" if grant["owner"] else "select"],
                )
            )
        for index, grant in enumerate(permissions.get("tables", []), start=1):
            policies.append(
                _policy(
                    service_name,
                    f"lakehouse-ops-{role_name}-table-{index}",
                    {
                        "catalog": _resource(grant["catalogs"]),
                        "schema": _resource(grant["schemas"]),
                        "table": _resource(grant["tables"]),
                        "column": _resource(["*"]),
                    },
                    users,
                    _table_accesses(grant["privileges"]),
                )
            )
        for index, grant in enumerate(permissions.get("row_filters", []), start=1):
            policies.append(
                _row_filter_policy(
                    service_name,
                    f"lakehouse-ops-{role_name}-row-filter-{index}",
                    {
                        "catalog": _resource(grant["catalogs"]),
                        "schema": _resource(grant["schemas"]),
                        "table": _resource(grant["tables"]),
                    },
                    users,
                    grant["filter"],
                )
            )
        for index, grant in enumerate(permissions.get("column_masks", []), start=1):
            policies.append(
                _data_mask_policy(
                    service_name,
                    f"lakehouse-ops-{role_name}-column-mask-{index}",
                    {
                        "catalog": _resource(grant["catalogs"]),
                        "schema": _resource(grant["schemas"]),
                        "table": _resource(grant["tables"]),
                        "column": _resource(grant["columns"]),
                    },
                    users,
                    grant["mask_type"],
                )
            )
        query_accesses = permissions.get("queries", [])
        if "execute" in query_accesses:
            policies.append(
                _policy(
                    service_name,
                    f"lakehouse-ops-{role_name}-query",
                    {"queryid": _resource(["*"])},
                    users,
                    ["execute"],
                )
            )
        system_accesses = permissions.get("system_information", [])
        if system_accesses:
            policies.append(
                _policy(
                    service_name,
                    f"lakehouse-ops-{role_name}-sysinfo",
                    {"sysinfo": _resource(["*"])},
                    users,
                    [f"{access}_sysinfo" for access in system_accesses],
                )
            )
    if "execute" in model["defaults"].get("queries", []):
        policies.append(
            _policy(
                service_name,
                "lakehouse-ops-default-query-execution",
                {"queryid": _resource(["*"])},
                all_users,
                ["execute"],
            )
        )
    policies.append(
        _policy(
            service_name,
            "lakehouse-ops-self-impersonation",
            {"trinouser": _resource(["{USER}"])},
            all_users,
            ["impersonate"],
        )
    )
    access_policies = [policy for policy in policies if policy["policyType"] == 0]
    transformation_policies = [
        policy for policy in policies if policy["policyType"] in {1, 2}
    ]
    return sorted(
        [*_merge_matching_resources(access_policies), *transformation_policies],
        key=lambda policy: policy["name"],
    )


class RangerAdminClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            auth=(username, password),
            timeout=15,
            transport=transport,
        )

    def __enter__(self) -> RangerAdminClient:
        return self

    def __exit__(self, *args: object) -> None:
        self._client.close()

    def sync(
        self,
        *,
        model_path: Path,
        service_name: str,
        trino_jdbc_url: str,
        service_user: str,
        break_glass_path: Path | None = None,
    ) -> dict[str, Any]:
        model = load_access_policy(model_path)
        break_glass = None
        if break_glass_path is not None:
            model, break_glass = apply_break_glass_lease(model, break_glass_path)
        service_status = self._ensure_service(
            service_name, trino_jdbc_url=trino_jdbc_url, service_user=service_user
        )
        users = sorted(
            {
                user
                for binding in model["bindings"]
                for user in binding["users"]
            }
        )
        user_status = self._ensure_users(users)
        desired = compile_ranger_policies(model, service_name)
        existing, bootstrap_deleted = self._remove_bootstrap_policies(
            service_name, service_user
        )
        existing_by_name = {
            policy["name"]: policy for policy in existing if isinstance(policy, dict)
        }
        consumed_ids: set[int] = set()
        created = updated = unchanged = deleted = 0
        desired_names = {policy["name"] for policy in desired}
        for policy in desired:
            current = existing_by_name.get(policy["name"])
            if current is None:
                current = next(
                    (
                        candidate
                        for candidate in existing
                        if isinstance(candidate, dict)
                        and candidate.get("description") == MANAGED_DESCRIPTION
                        and candidate.get("resources") == policy["resources"]
                        and candidate.get("id") not in consumed_ids
                    ),
                    None,
                )
            if current is None:
                try:
                    self._request("POST", "/service/public/v2/api/policy", json=policy)
                except RangerAdminError as error:
                    if "Another policy already exists for matching resource" not in str(
                        error
                    ):
                        raise
                    _, late_bootstrap_deleted = self._remove_bootstrap_policies(
                        service_name, service_user
                    )
                    if late_bootstrap_deleted == 0:
                        raise
                    bootstrap_deleted += late_bootstrap_deleted
                    self._request("POST", "/service/public/v2/api/policy", json=policy)
                created += 1
            elif _policy_projection(current) == _policy_projection(policy):
                consumed_ids.add(current["id"])
                unchanged += 1
            else:
                payload = {**policy, "id": current["id"]}
                self._request(
                    "PUT", f"/service/public/v2/api/policy/{current['id']}", json=payload
                )
                consumed_ids.add(current["id"])
                updated += 1
        for current in existing:
            if (
                isinstance(current, dict)
                and current.get("description") == MANAGED_DESCRIPTION
                and current.get("name") not in desired_names
                and current.get("id") not in consumed_ids
            ):
                self._request("DELETE", f"/service/public/v2/api/policy/{current['id']}")
                deleted += 1
        report = {
            "schema_version": "1.0",
            "status": "synchronized",
            "service": service_name,
            "service_status": service_status,
            "users": user_status,
            "policies": {
                "desired": len(desired),
                "bootstrap_deleted": bootstrap_deleted,
                "created": created,
                "updated": updated,
                "unchanged": unchanged,
                "deleted": deleted,
            },
        }
        if break_glass is not None:
            report["break_glass"] = break_glass
        return report

    def _remove_bootstrap_policies(
        self, service_name: str, service_user: str
    ) -> tuple[list[dict[str, Any]], int]:
        existing = self._request(
            "GET", f"/service/public/v2/api/service/{service_name}/policy"
        )
        bootstrap_policies = [
            policy
            for policy in existing
            if isinstance(policy, dict)
            and _is_ranger_bootstrap_policy(policy, service_user)
        ]
        for policy in bootstrap_policies:
            self._request("DELETE", f"/service/public/v2/api/policy/{policy['id']}")
        return (
            [policy for policy in existing if policy not in bootstrap_policies],
            len(bootstrap_policies),
        )

    def _ensure_service(
        self, service_name: str, *, trino_jdbc_url: str, service_user: str
    ) -> str:
        desired = {
            "name": service_name,
            "displayName": "Lakehouse Trino",
            "type": "trino",
            "isEnabled": True,
            "configs": {
                "username": service_user,
                "jdbc.driverClassName": "io.trino.jdbc.TrinoDriver",
                "jdbc.url": trino_jdbc_url,
            },
        }
        response = self._send(
            "GET", f"/service/public/v2/api/service/name/{service_name}"
        )
        if response.status_code == 404:
            self._request("POST", "/service/public/v2/api/service", json=desired)
            return "created"
        self._raise_for_status(response)
        current = response.json()
        current_configs = current.get("configs", {})
        if (
            current.get("type") == "trino"
            and current.get("isEnabled") is True
            and all(
                current_configs.get(key) == value
                for key, value in desired["configs"].items()
            )
        ):
            return "unchanged"
        self._request(
            "PUT",
            f"/service/public/v2/api/service/{current['id']}",
            json={**desired, "id": current["id"]},
        )
        return "updated"

    def _ensure_users(self, desired_users: list[str]) -> dict[str, int]:
        response = self._request(
            "GET", "/service/xusers/users", params={"pageSize": max(len(desired_users), 100)}
        )
        existing = {
            user["name"]
            for user in response.get("vXUsers", [])
            if isinstance(user, dict) and isinstance(user.get("name"), str)
        }
        created = 0
        for user in desired_users:
            if user in existing:
                continue
            self._request(
                "POST", "/service/xusers/users/external", json={"name": user}
            )
            created += 1
        return {
            "desired": len(desired_users),
            "created": created,
            "unchanged": len(desired_users) - created,
        }

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self._send(method, path, **kwargs)
        self._raise_for_status(response)
        if not response.content:
            return None
        return response.json()

    def _send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            return self._client.request(method, path, **kwargs)
        except httpx.HTTPError as error:
            raise RangerAdminError(
                f"Ranger Admin {method} {path} failed: {error}"
            ) from error

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        try:
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise RangerAdminError(
                f"Ranger Admin {response.request.method} {response.request.url.path} "
                f"failed with {response.status_code}: {response.text}"
            ) from error


def _policy(
    service: str,
    name: str,
    resources: dict[str, dict[str, Any]],
    users: list[str],
    accesses: list[str],
) -> dict[str, Any]:
    return {
        "service": service,
        "name": name,
        "description": MANAGED_DESCRIPTION,
        "policyType": 0,
        "isEnabled": True,
        "isAuditEnabled": True,
        "resources": resources,
        "policyItems": [
            {
                "users": sorted(users),
                "accesses": [
                    {"type": access, "isAllowed": True} for access in sorted(set(accesses))
                ],
                "delegateAdmin": False,
            }
        ],
        "isDenyAllElse": False,
    }


def _row_filter_policy(
    service: str,
    name: str,
    resources: dict[str, dict[str, Any]],
    users: list[str],
    filter_expression: str,
) -> dict[str, Any]:
    return {
        "service": service,
        "name": name,
        "description": MANAGED_DESCRIPTION,
        "policyType": 2,
        "isEnabled": True,
        "isAuditEnabled": True,
        "resources": resources,
        "policyItems": [],
        "rowFilterPolicyItems": [
            {
                "users": sorted(users),
                "accesses": [{"type": "select", "isAllowed": True}],
                "rowFilterInfo": {"filterExpr": filter_expression},
                "delegateAdmin": False,
            }
        ],
        "isDenyAllElse": False,
    }


def _data_mask_policy(
    service: str,
    name: str,
    resources: dict[str, dict[str, Any]],
    users: list[str],
    mask_type: str,
) -> dict[str, Any]:
    return {
        "service": service,
        "name": name,
        "description": MANAGED_DESCRIPTION,
        "policyType": 1,
        "isEnabled": True,
        "isAuditEnabled": True,
        "resources": resources,
        "policyItems": [],
        "dataMaskPolicyItems": [
            {
                "users": sorted(users),
                "accesses": [{"type": "select", "isAllowed": True}],
                "dataMaskInfo": {"dataMaskType": mask_type},
                "delegateAdmin": False,
            }
        ],
        "isDenyAllElse": False,
    }


def _resource(values: list[str]) -> dict[str, Any]:
    return {"values": values, "isExcludes": False, "isRecursive": False}


def _users_by_role(model: dict[str, Any]) -> dict[str, list[str]]:
    result = {role: [] for role in model["roles"]}
    for binding in model["bindings"]:
        for role in binding["roles"]:
            for user in binding["users"]:
                if user not in result[role]:
                    result[role].append(user)
    return result


def _table_accesses(privileges: list[str]) -> list[str]:
    if "OWNERSHIP" in privileges:
        return ["all"]
    mapping = {
        "SELECT": "select",
        "INSERT": "insert",
        "DELETE": "delete",
        "UPDATE": "alter",
        "GRANT_SELECT": "grant",
    }
    return sorted({mapping[privilege] for privilege in privileges if privilege in mapping})


def _policy_projection(policy: dict[str, Any]) -> dict[str, Any]:
    projection = {
        key: policy.get(key)
        for key in (
            "service",
            "name",
            "description",
            "policyType",
            "isEnabled",
            "isAuditEnabled",
            "resources",
            "isDenyAllElse",
        )
    }
    for key in (
        "policyItems",
        "denyPolicyItems",
        "allowExceptions",
        "denyExceptions",
        "dataMaskPolicyItems",
        "rowFilterPolicyItems",
    ):
        projection[key] = policy.get(key) or []
    return projection


def _is_ranger_bootstrap_policy(policy: dict[str, Any], service_user: str) -> bool:
    items = policy.get("policyItems")
    return (
        isinstance(policy.get("name"), str)
        and policy["name"].startswith("all - ")
        and isinstance(policy.get("description"), str)
        and policy["description"].startswith("Policy for all - ")
        and isinstance(items, list)
        and bool(items)
        and all(
            isinstance(item, dict)
            and item.get("users") == [service_user]
            and item.get("delegateAdmin") is True
            for item in items
        )
    )


def _merge_matching_resources(policies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for policy in policies:
        key = json.dumps(policy["resources"], sort_keys=True, separators=(",", ":"))
        grouped.setdefault(key, []).append(policy)
    merged: list[dict[str, Any]] = []
    for key, matches in grouped.items():
        template = matches[0]
        items_by_access: dict[tuple[str, ...], set[str]] = {}
        for match in matches:
            item = match["policyItems"][0]
            accesses = tuple(sorted(access["type"] for access in item["accesses"]))
            items_by_access.setdefault(accesses, set()).update(item["users"])
        resources = template["resources"]
        policy_items = [
            {
                "users": sorted(users),
                "accesses": [
                    {"type": access, "isAllowed": True} for access in accesses
                ],
                "delegateAdmin": False,
            }
            for accesses, users in sorted(items_by_access.items())
        ]
        merged.append(
            {
                **template,
                "name": _merged_policy_name(resources, key),
                "policyItems": policy_items,
            }
        )
    return sorted(merged, key=lambda policy: policy["name"])


def _merged_policy_name(resources: dict[str, Any], resource_key: str) -> str:
    if set(resources) == {"queryid"}:
        return "lakehouse-ops-query-execution"
    if set(resources) == {"sysinfo"}:
        return "lakehouse-ops-system-information"
    if set(resources) == {"trinouser"}:
        return "lakehouse-ops-self-impersonation"
    resource_type = "-".join(sorted(resources))
    digest = hashlib.sha256(resource_key.encode()).hexdigest()[:10]
    return f"lakehouse-ops-{resource_type}-{digest}"
