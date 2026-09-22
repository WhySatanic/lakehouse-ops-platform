from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

import lakehouse_ops.ingestion as ingestion
from lakehouse_ops.ingestion.commerce_bronze import (
    CommerceBronzeError,
    load_commerce_batch,
)
from lakehouse_ops.ingestion.commerce_fixture import (
    CommerceFixtureConfig,
    generate_commerce_fixture,
)


def test_contract_module_does_not_load_optional_http_client() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; sys.modules['httpx'] = None; "
                "import lakehouse_ops.ingestion.commerce_bronze"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_ingestion_public_api_remains_available_through_lazy_imports() -> None:
    assert ingestion.Location.__name__ == "Location"
    assert ingestion.WeatherPayload.__name__ == "WeatherPayload"
    assert ingestion.OpenMeteoClient.__name__ == "OpenMeteoClient"
    assert ingestion.OpenMeteoError.__name__ == "OpenMeteoError"
    assert not hasattr(ingestion, "missing_export")


@pytest.fixture
def batch_path(tmp_path: Path) -> Path:
    return generate_commerce_fixture(
        tmp_path,
        CommerceFixtureConfig(
            customers=4,
            products=2,
            orders=6,
            null_customer_emails=1,
            duplicate_orders=1,
            late_orders=1,
            invalid_payments=1,
        ),
    ).path


def test_loads_checksum_verified_batch_directory(batch_path: Path) -> None:
    batch_id = batch_path.name.removeprefix("batch_id=")

    batch = load_commerce_batch(batch_path, expected_batch_id=batch_id)

    assert batch.batch_id == batch_id
    assert batch.batch_at.endswith("Z")
    assert [table.name for table in batch.tables] == [
        "customers",
        "orders",
        "payments",
        "products",
    ]
    assert {table.name: table.rows for table in batch.tables} == {
        "customers": 4,
        "orders": 7,
        "payments": 6,
        "products": 2,
    }


def test_rejects_batch_directory_identity_mismatch(batch_path: Path) -> None:
    with pytest.raises(CommerceBronzeError, match="directory does not match"):
        load_commerce_batch(batch_path, expected_batch_id="a" * 16)


def test_rejects_invalid_batch_id(batch_path: Path) -> None:
    with pytest.raises(CommerceBronzeError, match="16 lowercase hexadecimal"):
        load_commerce_batch(batch_path, expected_batch_id="not-a-batch")


def test_rejects_modified_table(batch_path: Path) -> None:
    (batch_path / "orders.jsonl").write_text("{}\n", encoding="utf-8")
    batch_id = batch_path.name.removeprefix("batch_id=")

    with pytest.raises(CommerceBronzeError, match="checksum mismatch"):
        load_commerce_batch(batch_path, expected_batch_id=batch_id)


def test_rejects_manifest_row_count_mismatch(batch_path: Path) -> None:
    manifest_path = batch_path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["tables"]["orders"]["rows"] += 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    batch_id = batch_path.name.removeprefix("batch_id=")

    with pytest.raises(CommerceBronzeError, match="row count mismatch"):
        load_commerce_batch(batch_path, expected_batch_id=batch_id)


def test_rejects_missing_required_table(batch_path: Path) -> None:
    manifest_path = batch_path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    del manifest["tables"]["payments"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    batch_id = batch_path.name.removeprefix("batch_id=")

    with pytest.raises(CommerceBronzeError, match="exactly four required tables"):
        load_commerce_batch(batch_path, expected_batch_id=batch_id)
