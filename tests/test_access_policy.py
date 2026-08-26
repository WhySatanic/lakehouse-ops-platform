from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakehouse_ops.access_policy import (
    AccessPolicyError,
    compile_trino_policy,
    load_access_policy,
    render_trino_policy,
)

ROOT = Path(__file__).parents[1]
MODEL_PATH = ROOT / "config" / "access" / "role-policy.json"
TRINO_POLICY_PATH = ROOT / "infra" / "trino" / "access-control-rules.json"


def test_checked_in_trino_policy_matches_role_model() -> None:
    model = load_access_policy(MODEL_PATH)

    assert compile_trino_policy(model) == json.loads(
        TRINO_POLICY_PATH.read_text(encoding="utf-8")
    )
    assert render_trino_policy(MODEL_PATH, TRINO_POLICY_PATH, check=True) is True


def test_compile_escapes_subjects_and_denies_unmatched_access() -> None:
    model = load_access_policy(MODEL_PATH)
    model["bindings"][0]["users"] = ["admin@example.com"]

    policy = compile_trino_policy(model)

    assert policy["catalogs"][0]["user"] == r"admin@example\.com"
    assert policy["catalogs"][-1] == {"catalog": ".*", "allow": "none"}
    assert policy["tables"][-1]["privileges"] == []
    assert policy["system_information"][-1]["allow"] == []


def test_render_reports_drift_then_writes_atomically(tmp_path: Path) -> None:
    output = tmp_path / "access-control-rules.json"
    output.write_text("{}\n", encoding="utf-8")

    assert render_trino_policy(MODEL_PATH, output, check=True) is False
    assert render_trino_policy(MODEL_PATH, output) is False
    assert render_trino_policy(MODEL_PATH, output, check=True) is True


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ([], "policy model must be a JSON object"),
        ({"schema_version": "2.0"}, "unsupported policy schema_version"),
        (
            {"schema_version": "1.0", "roles": {}, "bindings": [], "defaults": {}},
            "roles must be a non-empty object",
        ),
        (
            {
                "schema_version": "1.0",
                "roles": {"reader": {}},
                "bindings": [],
                "defaults": {},
            },
            "bindings must be a non-empty array",
        ),
        (
            {
                "schema_version": "1.0",
                "roles": {"reader": {}},
                "bindings": ["reader"],
                "defaults": {},
            },
            "each binding must be an object",
        ),
        (
            {
                "schema_version": "1.0",
                "roles": {"reader": {}},
                "bindings": [{"users": [], "roles": ["reader"]}],
                "defaults": {},
            },
            "binding users must be non-empty strings",
        ),
        (
            {
                "schema_version": "1.0",
                "roles": {"reader": {}},
                "bindings": [{"users": ["alice"], "roles": []}],
                "defaults": {},
            },
            "binding roles must be a non-empty array",
        ),
        (
            {
                "schema_version": "1.0",
                "roles": {"reader": {}},
                "bindings": [{"users": ["alice"], "roles": ["writer"]}],
                "defaults": {},
            },
            "binding references unknown role: writer",
        ),
    ],
)
def test_load_rejects_invalid_models(
    tmp_path: Path, document: object, message: str
) -> None:
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(AccessPolicyError, match=message):
        load_access_policy(path)


def test_load_rejects_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "policy.json"
    path.write_text("{", encoding="utf-8")

    with pytest.raises(AccessPolicyError, match="invalid policy JSON"):
        load_access_policy(path)


def test_compile_rejects_empty_resource_pattern() -> None:
    model = load_access_policy(MODEL_PATH)
    model["roles"]["analyst"]["tables"][0]["schemas"] = []

    with pytest.raises(AccessPolicyError, match="schemas must be a non-empty string array"):
        compile_trino_policy(model)
