from __future__ import annotations

import json

import pytest

from lakehouse_ops.ingestion.commerce_fixture import (
    CommerceFixtureConfig,
    generate_commerce_fixture,
)


def test_explicit_snapshot_keeps_legacy_identity_and_verifies_replay(tmp_path):
    config = CommerceFixtureConfig(
        customers=10, products=5, orders=100, null_customer_emails=1,
        duplicate_orders=1, late_orders=1, invalid_payments=1,
        batch_at="2026-02-01T00:00:00+00:00",
    )
    legacy = generate_commerce_fixture(tmp_path / "legacy", config)
    assert legacy.batch_id == "271bf513fa34b667"
    manifest = json.loads((legacy.path / "manifest.json").read_text())
    assert "source_semantics" not in manifest["config"]
    snapshot = generate_commerce_fixture(tmp_path / "snapshot", config,
                                         source_semantics="full_snapshot")
    assert snapshot.batch_id != legacy.batch_id
    assert json.loads((snapshot.path / "manifest.json").read_text())["config"][
        "source_semantics"
    ] == "full_snapshot"
    for table in ("customers", "products", "orders", "payments"):
        assert (snapshot.path / f"{table}.jsonl").read_bytes() == (
            legacy.path / f"{table}.jsonl"
        ).read_bytes()
    assert not generate_commerce_fixture(tmp_path / "snapshot", config,
                                        source_semantics="full_snapshot").created


def test_unknown_source_semantics_creates_no_fixture(tmp_path):
    with pytest.raises(ValueError, match="source semantics"):
        generate_commerce_fixture(tmp_path, CommerceFixtureConfig(), source_semantics="delta")
    assert list(tmp_path.iterdir()) == []
