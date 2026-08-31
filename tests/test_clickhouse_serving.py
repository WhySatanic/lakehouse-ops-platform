from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
CHECK_PATH = ROOT / "tests" / "integration" / "check_clickhouse_serving.py"
COMPOSE_PATH = ROOT / "compose.yaml"


def _load_checker():
    spec = importlib.util.spec_from_file_location("check_clickhouse_serving", CHECK_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_acceptance_contract_accepts_complete_evidence() -> None:
    checker = _load_checker()
    checker.validate(dict(checker.EXPECTED_REPORT))


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("status", "degraded"),
        ("mode", "native_copy"),
        ("clickhouse_version", "latest"),
        ("silver_rows", 1),
        ("duplicate_keys", 1),
        ("latest_survivor", 0),
    ],
)
def test_acceptance_contract_rejects_changed_evidence(key: str, value: object) -> None:
    checker = _load_checker()
    report = dict(checker.EXPECTED_REPORT)
    report[key] = value

    with pytest.raises(ValueError, match=key):
        checker.validate(report)


def test_acceptance_contract_rejects_non_object() -> None:
    checker = _load_checker()

    with pytest.raises(ValueError, match="must be an object"):
        checker.validate([])


def test_serving_profile_is_pinned_and_read_only() -> None:
    compose = COMPOSE_PATH.read_text(encoding="utf-8")
    assert "clickhouse/clickhouse-server:26.3.25.2" in compose
    assert 'profiles: ["serving"]' in compose
    assert "MINIO_CLICKHOUSE_USER" in compose
    assert "MINIO_ROOT_USER" not in compose[compose.index("  clickhouse-server:") :]
    policy = json.loads(
        (ROOT / "config" / "s3" / "clickhouse-policy.json").read_text(
            encoding="utf-8"
        )
    )
    actions = {
        action
        for statement in policy["Statement"]
        for action in statement["Action"]
    }
    assert actions == {"s3:GetBucketLocation", "s3:ListBucket", "s3:GetObject"}
