from __future__ import annotations

import json
import sys
from contextlib import nullcontext

import pytest

from lakehouse_ops import commerce_query_observations as observation
from lakehouse_ops.commerce_pipeline_profile import query_costs
from lakehouse_ops.trino import TrinoQueryResult, TrinoQueryStats


class Client:
    def __init__(self, failure=None):
        self.failure = failure
        self.statements = []
        self.inventories = 0

    def query(self, sql):
        if "information_schema" in sql:
            self.inventories += 1
            return [{"table_schema": "gold", "table_name": "commerce_daily"}]
        return [{"snapshot_id": self.inventories if self.failure == "snapshot" else 1}]

    def query_with_stats(self, sql):
        self.statements.append(sql)
        index = len(self.statements)
        if self.failure == "process" and index == 5:
            raise RuntimeError("Trino disconnected")
        revenue = 12345 + (1 if self.failure == "changed" and index >= 5 else 0)
        # Warm-up costs are intentionally much higher and must not enter medians.
        elapsed = 1000 if index <= 2 else index
        return TrinoQueryResult(str(index), ({"orders": 100, "revenue": revenue},),
                                TrinoQueryStats("FINISHED", elapsed, 3, 4, 5, 6, 7, 8, 0))


@pytest.mark.parametrize("failure", [None, "changed", "snapshot", "checkpoint", "process"])
def test_repeated_reads_balance_order_exclude_warmup_and_retain_failures(
    tmp_path, failure,
):
    state = tmp_path / "checkpoint.json"
    state.write_bytes(b"original checkpoint")
    client = Client(failure)
    original_query = client.query

    def query(sql):
        result = original_query(sql)
        if failure == "checkpoint" and client.inventories == 2:
            state.write_bytes(b"external writer")
        return result

    client.query = query
    output = tmp_path / "evidence"
    if failure:
        with pytest.raises((ValueError, RuntimeError)):
            observation.observe(client, output, state, batch_id="a" * 16, orders=100)
    else:
        report = observation.observe(client, output, state, batch_id="a" * 16, orders=100)
        assert report["status"] == "ready"
        assert report["measured_queries"] == 8 and report["warmup_queries"] == 2
        assert [sample["order"] for sample in report["samples"]] == [
            ["detail", "daily"], ["daily", "detail"],
            ["detail", "daily"], ["daily", "detail"],
        ]
        assert report["metrics"]["detail"]["elapsed_time_ms"] == {
            "median": 6.5, "min": 3, "max": 10,
        }
        assert report["metrics"]["daily"]["elapsed_time_ms"] == {
            "median": 6.5, "min": 4, "max": 9,
        }
        assert set(report["metrics"]["daily"]) == set(observation.METRICS)
        assert state.read_bytes() == b"original checkpoint"
        assert len(client.statements) == 10
        assert all(sql.startswith(("WITH ", "SELECT ")) for sql in client.statements)
    retained = json.loads((output / "queries.json").read_text())
    assert retained["status"] == ("failed" if failure else "ready")
    if failure == "process":
        assert len(retained["samples"]) == 1
    if failure == "changed":
        assert len(retained["samples"]) == 2


@pytest.mark.parametrize("repetitions", [0, 1, 3, 11, True])
def test_bad_repetitions_do_not_query_or_create_output(tmp_path, repetitions):
    with pytest.raises(ValueError, match="balanced"):
        observation.observe(None, tmp_path / "output", tmp_path / "missing",
                            batch_id="a" * 16, orders=100, repetitions=repetitions)
    assert not (tmp_path / "output").exists()


def test_daily_first_executes_daily_before_detail_without_swapping_results():
    client = Client()
    result = query_costs(client, "a" * 16, 100, daily_first=True)
    assert "commerce_daily" in client.statements[0]
    assert "commerce_orders" in client.statements[1]
    assert result["daily"]["query_id"] == "1" and result["detail"]["query_id"] == "2"


@pytest.mark.parametrize("failed", [False, True])
def test_main_binds_query_timeout_and_reports_failure(tmp_path, monkeypatch, failed):
    state = tmp_path / "state"
    state.write_bytes(b"checkpoint")
    monkeypatch.setattr(sys, "argv", ["queries", "--output", str(tmp_path / "output"),
                                     "--state", str(state), "--server", "http://localhost:123",
                                     "--batch-id", "a" * 16, "--orders", "100"])

    def client(server, **kwargs):
        assert kwargs["query_timeout"] == 60
        return nullcontext(Client("process" if failed else None))

    monkeypatch.setattr(observation, "TrinoClient", client)
    assert observation.main() == (2 if failed else 0)
