from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
POLICY_DIR = ROOT / "config" / "s3"
COMPOSE_PATH = ROOT / "compose.yaml"


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
    assert "arn:aws:s3:::__BUCKET__/landing" in _values(policy, "Resource")
    assert "arn:aws:s3:::__BUCKET__/landing/*" in _values(policy, "Resource")
    assert "arn:aws:s3:::__BUCKET__/warehouse/*" in _values(policy, "Resource")


def test_trino_policy_is_warehouse_read_only() -> None:
    policy = _policy("trino")
    assert _values(policy, "Action") == {"s3:GetBucketLocation", "s3:ListBucket", "s3:GetObject"}
    assert "arn:aws:s3:::__BUCKET__/warehouse" in _values(policy, "Resource")
    assert all("landing" not in value for value in _values(policy, "Resource"))


def test_policies_do_not_grant_wildcards() -> None:
    for name in ("ingest", "spark", "trino"):
        policy = _policy(name)
        assert "*" not in _values(policy, "Action")
        assert "*" not in _values(policy, "Resource")


def test_runtime_services_do_not_use_minio_root_credentials() -> None:
    compose = COMPOSE_PATH.read_text(encoding="utf-8")
    runtime_services = (
        "landing-fixture",
        "silver-landing-fixture",
        "bronze-input-sync",
        "spark-bronze",
        "spark-silver",
        "trino-coordinator",
        "trino-worker",
        "trino-worker-2",
    )

    for index, service in enumerate(runtime_services):
        start = compose.index(f"  {service}:")
        following = [
            compose.find(f"  {candidate}:", start + 1)
            for candidate in runtime_services[index + 1 :]
        ]
        end = min((position for position in following if position >= 0), default=len(compose))
        block = compose[start:end]
        assert "MINIO_ROOT_USER" not in block
        assert "MINIO_ROOT_PASSWORD" not in block
