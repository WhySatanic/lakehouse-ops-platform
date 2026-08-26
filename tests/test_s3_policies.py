from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
POLICY_DIR = ROOT / "config" / "s3"


def _policy(name: str) -> dict[str, object]:
    return json.loads((POLICY_DIR / f"{name}-policy.json").read_text(encoding="utf-8"))


def _values(policy: dict[str, object], key: str) -> set[str]:
    return {value for statement in policy["Statement"] for value in statement[key]}


def test_ingest_policy_is_limited_to_landing() -> None:
    policy = _policy("ingest")
    assert "s3:PutObject" in _values(policy, "Action")
    assert "arn:aws:s3:::__BUCKET__/landing/*" in _values(policy, "Resource")
    assert all("warehouse" not in value for value in _values(policy, "Resource"))


def test_spark_policy_reads_landing_and_owns_warehouse_objects() -> None:
    policy = _policy("spark")
    assert {"s3:GetObject", "s3:PutObject", "s3:DeleteObject"} <= _values(policy, "Action")
    assert "arn:aws:s3:::__BUCKET__/landing/*" in _values(policy, "Resource")
    assert "arn:aws:s3:::__BUCKET__/warehouse/*" in _values(policy, "Resource")


def test_trino_policy_is_warehouse_read_only() -> None:
    policy = _policy("trino")
    assert _values(policy, "Action") == {"s3:GetBucketLocation", "s3:ListBucket", "s3:GetObject"}
    assert all("landing" not in value for value in _values(policy, "Resource"))


def test_policies_do_not_grant_wildcards() -> None:
    for name in ("ingest", "spark", "trino"):
        policy = _policy(name)
        assert "*" not in _values(policy, "Action")
        assert "*" not in _values(policy, "Resource")
