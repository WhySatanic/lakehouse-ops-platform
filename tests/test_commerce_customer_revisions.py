from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from lakehouse_ops import cli
from lakehouse_ops.ingestion.commerce_fixture import (
    CommerceFixtureConfig,
    CommerceFixtureError,
    generate_commerce_fixture,
)
from lakehouse_ops.ingestion.commerce_s3_landing import _load_fixture


def config():
    return CommerceFixtureConfig(customers=2, products=2, orders=3, null_customer_emails=1,
                                 duplicate_orders=1, late_orders=1, invalid_payments=1, seed=11)


def customers(result):
    return [json.loads(line) for line in (result.path / "customers.jsonl").read_text().splitlines()]


def test_zero_revision_preserves_existing_identity_manifest_and_payload(tmp_path):
    default = generate_commerce_fixture(tmp_path, config())
    explicit = generate_commerce_fixture(tmp_path, config(), customer_revision=0)
    assert default.batch_id == explicit.batch_id == "c94e5c49fdb78e7f"
    assert not explicit.created
    manifest = json.loads((default.path / "manifest.json").read_text())
    assert "customer_revision" not in manifest["config"]
    assert manifest["tables"]["customers"]["sha256"] == (
        "c471ab48e5c45a4ba347be552a361d8bedbcad19bb0c2cc690c9278f04fdb1e3"
    )


def test_revision_changes_only_names_and_identity_with_verified_replay(tmp_path):
    original = generate_commerce_fixture(tmp_path, config())
    changed = generate_commerce_fixture(tmp_path, config(), customer_revision=7)
    assert changed.batch_id != original.batch_id
    assert generate_commerce_fixture(tmp_path, config(), customer_revision=7).created is False
    for table in ("orders", "payments", "products"):
        assert (original.path / f"{table}.jsonl").read_bytes() == (
            changed.path / f"{table}.jsonl"
        ).read_bytes()
    before, after = customers(original), customers(changed)
    assert [row["full_name"] for row in after] == [
        "Customer 00000001 revision 7", "Customer 00000002 revision 7",
    ]
    for old, new in zip(before, after, strict=True):
        old.pop("full_name")
        new.pop("full_name")
        assert old == new
    manifest, objects, _ = _load_fixture(changed.path)
    assert manifest["config"]["customer_revision"] == 7
    assert len(objects) == 4
    damaged = changed.path / "customers.jsonl"
    damaged.write_bytes(b"damaged\n")
    with pytest.raises(CommerceFixtureError, match="checksum"):
        generate_commerce_fixture(tmp_path, config(), customer_revision=7)
    assert damaged.read_bytes() == b"damaged\n"


def test_chronological_inputs_express_change_then_no_change_then_change(tmp_path):
    rows = []
    for day, revision in enumerate((0, 1, 1, 2), start=1):
        batch = generate_commerce_fixture(
            tmp_path, replace(config(), batch_at=f"2026-02-0{day}T00:00:00Z"),
            customer_revision=revision, source_semantics="full_snapshot",
        )
        manifest, _, _ = _load_fixture(batch.path)
        assert manifest["config"]["source_semantics"] == "full_snapshot"
        rows.append(customers(batch))
    assert [snapshot[1]["full_name"] for snapshot in rows] == [
        "Customer 00000002", "Customer 00000002 revision 1",
        "Customer 00000002 revision 1", "Customer 00000002 revision 2",
    ]
    assert all(snapshot[0]["email"] is None for snapshot in rows)
    assert all(snapshot[1]["customer_id"] == "C00000002" for snapshot in rows)


@pytest.mark.parametrize("revision", [-1, 10001, True, 1.0, "1", None])
def test_invalid_revision_fails_before_creating_output(tmp_path, revision):
    output = tmp_path / "new"
    with pytest.raises(CommerceFixtureError, match="customer revision"):
        generate_commerce_fixture(output, config(), customer_revision=revision)
    assert not output.exists()


def test_cli_can_generate_explicit_full_snapshot_with_customer_revision(tmp_path, capsys):
    assert cli.main([
        "generate-commerce-fixture", "--output", str(tmp_path),
        "--customers", "2", "--products", "2", "--orders", "3",
        "--null-customer-emails", "1", "--duplicate-orders", "1",
        "--late-orders", "1", "--invalid-payments", "1",
        "--source-semantics", "full_snapshot", "--customer-revision", "3",
    ]) == 0
    result = json.loads(capsys.readouterr().out)
    manifest, _, _ = _load_fixture(Path(result["path"]))
    assert manifest["config"]["customer_revision"] == 3
    assert manifest["config"]["source_semantics"] == "full_snapshot"
