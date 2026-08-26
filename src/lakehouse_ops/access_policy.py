from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any


class AccessPolicyError(ValueError):
    pass


def load_access_policy(path: Path) -> dict[str, Any]:
    try:
        model = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise AccessPolicyError(f"invalid policy JSON: {error}") from error
    if not isinstance(model, dict):
        raise AccessPolicyError("policy model must be a JSON object")
    if model.get("schema_version") != "1.0":
        raise AccessPolicyError("unsupported policy schema_version")
    roles = model.get("roles")
    bindings = model.get("bindings")
    defaults = model.get("defaults")
    if not isinstance(roles, dict) or not roles:
        raise AccessPolicyError("roles must be a non-empty object")
    if not isinstance(bindings, list) or not bindings:
        raise AccessPolicyError("bindings must be a non-empty array")
    if not isinstance(defaults, dict):
        raise AccessPolicyError("defaults must be an object")
    for binding in bindings:
        if not isinstance(binding, dict):
            raise AccessPolicyError("each binding must be an object")
        users = binding.get("users")
        assigned_roles = binding.get("roles")
        if not isinstance(users, list) or not users or not all(_is_name(x) for x in users):
            raise AccessPolicyError("binding users must be non-empty strings")
        if not isinstance(assigned_roles, list) or not assigned_roles:
            raise AccessPolicyError("binding roles must be a non-empty array")
        unknown = [name for name in assigned_roles if name not in roles]
        if unknown:
            raise AccessPolicyError(f"binding references unknown role: {unknown[0]}")
    return model


def compile_trino_policy(model: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    roles = model["roles"]
    users_by_role = _users_by_role(model["bindings"], roles)
    output: dict[str, list[dict[str, Any]]] = {
        "catalogs": [],
        "schemas": [],
        "tables": [],
        "queries": [],
        "system_information": [],
        "system_session_properties": [],
        "catalog_session_properties": [],
    }
    for role_name, permissions in roles.items():
        user_pattern = _pattern(users_by_role.get(role_name, []))
        if not user_pattern:
            continue
        _compile_role(output, user_pattern, permissions)
    _append_defaults(output, model["defaults"])
    return output


def render_trino_policy(model_path: Path, output_path: Path, *, check: bool = False) -> bool:
    rendered = _serialize(compile_trino_policy(load_access_policy(model_path)))
    current = output_path.read_text(encoding="utf-8") if output_path.exists() else None
    if check:
        return current == rendered
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=output_path.parent, delete=False
    ) as handle:
        handle.write(rendered)
        temporary_path = Path(handle.name)
    os.replace(temporary_path, output_path)
    return current == rendered


def _compile_role(
    output: dict[str, list[dict[str, Any]]],
    user_pattern: str,
    permissions: dict[str, Any],
) -> None:
    for grant in permissions.get("catalogs", []):
        output["catalogs"].append(
            {
                "user": user_pattern,
                "catalog": _required_pattern(grant, "catalogs"),
                "allow": grant["access"],
            }
        )
    for grant in permissions.get("schemas", []):
        output["schemas"].append(
            {
                "user": user_pattern,
                "catalog": _required_pattern(grant, "catalogs"),
                "schema": _required_pattern(grant, "schemas"),
                "owner": grant["owner"],
            }
        )
    for grant in permissions.get("tables", []):
        output["tables"].append(
            {
                "user": user_pattern,
                "catalog": _required_pattern(grant, "catalogs"),
                "schema": _required_pattern(grant, "schemas"),
                "table": _required_pattern(grant, "tables"),
                "privileges": grant["privileges"],
            }
        )
    if "queries" in permissions:
        output["queries"].append({"user": user_pattern, "allow": permissions["queries"]})
    if "system_information" in permissions:
        output["system_information"].append(
            {"user": user_pattern, "allow": permissions["system_information"]}
        )
    if permissions.get("system_session_properties"):
        output["system_session_properties"].append(
            {"user": user_pattern, "property": ".*", "allow": True}
        )
    if permissions.get("catalog_session_properties"):
        output["catalog_session_properties"].append(
            {"user": user_pattern, "catalog": ".*", "property": ".*", "allow": True}
        )


def _append_defaults(
    output: dict[str, list[dict[str, Any]]], defaults: dict[str, Any]
) -> None:
    output["catalogs"].extend(
        [
            {"catalog": "system", "allow": defaults["system_catalog_access"]},
            {"catalog": ".*", "allow": defaults["catalog_access"]},
        ]
    )
    output["schemas"].append({"catalog": ".*", "schema": ".*", "owner": False})
    output["tables"].append(
        {"catalog": ".*", "schema": ".*", "table": ".*", "privileges": []}
    )
    output["queries"].append({"allow": defaults["queries"]})
    output["system_information"].append({"allow": defaults["system_information"]})
    output["system_session_properties"].append({"property": ".*", "allow": False})
    output["catalog_session_properties"].append(
        {"catalog": ".*", "property": ".*", "allow": False}
    )


def _users_by_role(
    bindings: list[dict[str, Any]], roles: dict[str, Any]
) -> dict[str, list[str]]:
    result = {name: [] for name in roles}
    for binding in bindings:
        for role_name in binding["roles"]:
            for user in binding["users"]:
                if user not in result[role_name]:
                    result[role_name].append(user)
    return result


def _required_pattern(grant: dict[str, Any], key: str) -> str:
    values = grant.get(key)
    if not isinstance(values, list) or not values or not all(_is_name(x) for x in values):
        raise AccessPolicyError(f"{key} must be a non-empty string array")
    return _pattern(values)


def _pattern(values: list[str]) -> str:
    return "|".join(".*" if value == "*" else re.escape(value) for value in values)


def _is_name(value: object) -> bool:
    return isinstance(value, str) and bool(value)


def _serialize(policy: dict[str, Any]) -> str:
    return json.dumps(policy, indent=2, ensure_ascii=False) + "\n"
