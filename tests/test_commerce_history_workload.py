from __future__ import annotations

import json
from datetime import datetime, timedelta
from itertools import pairwise

import pytest

from lakehouse_ops.commerce_pipeline_profile import profile, workload_configs
from lakehouse_ops.ingestion.commerce_fixture import generate_commerce_fixture


@pytest.mark.parametrize(("customers", "batches"), [(10, 2), (10_000, 50), (10_000, 100)])
def test_workload_calendar_rollover_and_independent_cardinality(customers, batches):
    configs = workload_configs([100, 1000], customers=customers, batches=batches)
    assert len(configs) == batches
    assert [config.orders for config in configs] == [100] + [1000] * (batches - 1)
    assert {config.customers for config in configs} == {customers}
    dates = [datetime.fromisoformat(config.batch_at) for config in configs]
    assert all(b - a == timedelta(days=1) for a, b in pairwise(dates))
    assert dates[-1] - dates[0] == timedelta(days=batches - 1)


@pytest.mark.parametrize("kwargs", [{"customers": 1}, {"customers": 100001},
                                   {"customers": True}, {"batches": 1},
                                   {"batches": 101}, {"batches": 2.5}])
def test_invalid_workload_creates_no_output(tmp_path, kwargs):
    output = tmp_path / "evidence"
    with pytest.raises(ValueError):
        profile(output, orders=[100, 1000], endpoint="unused", server="unused", **kwargs)
    assert not output.exists()


def test_real_large_dimension_snapshots_keep_links_and_stable_attributes(tmp_path):
    configs = workload_configs([100, 100], customers=10000, batches=100)
    results = [generate_commerce_fixture(tmp_path / str(index), configs[index],
                                         source_semantics="full_snapshot") for index in (0, 99)]
    assert results[0].batch_id != results[1].batch_id
    customer_rows = []
    for result in results:
        manifest = json.loads((result.path / "manifest.json").read_text())
        assert manifest["config"]["source_semantics"] == "full_snapshot"
        assert manifest["tables"]["customers"]["rows"] == 10000
        customers = [json.loads(line) for line in (result.path / "customers.jsonl").read_text(
            encoding="utf-8",
        ).splitlines()]
        ids = {row["customer_id"] for row in customers}
        assert len(ids) == 10000
        assert sum(row["email"] is None for row in customers) == 1
        orders = [json.loads(line) for line in (result.path / "orders.jsonl").read_text(
            encoding="utf-8",
        ).splitlines()]
        assert all(row["customer_id"] in ids for row in orders)
        customer_rows.append(customers)
    assert customer_rows[0] == customer_rows[1]  # Explicitly no SCD2 attribute version growth.
