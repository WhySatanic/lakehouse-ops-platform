from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import boto3
import pytest

from lakehouse_ops.ingestion.commerce_batches import CommerceBatchError, CommerceBatchPlanner
from lakehouse_ops.ingestion.commerce_fixture import (
    CommerceFixtureConfig,
    generate_commerce_fixture,
)
from lakehouse_ops.ingestion.commerce_s3_landing import CommerceS3LandingZone


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_S3_SMOKE") != "1", reason="requires an initialized local MinIO"
)
def test_real_minio_checkpoint_cannot_skip_older_batch(tmp_path: Path) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("LAKEOPS_S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "lakeops"),
        aws_secret_access_key=os.getenv(
            "MINIO_ROOT_PASSWORD", "lakeops-development-only"
        ),
        region_name="us-east-1",
    )
    bucket = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
    prefix = f"smoke/commerce-checkpoint-{uuid4().hex}"
    landing = CommerceS3LandingZone(client, bucket=bucket, prefix=prefix)
    config = {
        "customers": 4,
        "products": 2,
        "orders": 6,
        "null_customer_emails": 1,
        "duplicate_orders": 1,
        "late_orders": 1,
        "invalid_payments": 1,
    }
    later = generate_commerce_fixture(
        tmp_path / "fixture", CommerceFixtureConfig(**config, batch_at="2026-02-01T00:00:00+00:00")
    )
    earlier = generate_commerce_fixture(
        tmp_path / "fixture", CommerceFixtureConfig(**config, batch_at="2026-01-01T00:00:00+00:00")
    )
    landing.write(later.path)
    landing.write(earlier.path)
    state_path = tmp_path / "checkpoint.json"
    planner = CommerceBatchPlanner(
        client, bucket=bucket, prefix=prefix, state_path=state_path
    )

    with pytest.raises(CommerceBatchError, match="earlier unprocessed batch"):
        planner.commit(later.batch_id)

    assert not state_path.exists()
    assert planner.commit(earlier.batch_id)["created"] is True
    assert planner.commit(later.batch_id)["created"] is True
    assert planner.plan(max_batches=1)["selected_batches"] == 0
