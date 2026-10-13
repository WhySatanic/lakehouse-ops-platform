from __future__ import annotations

import pytest

from lakehouse_ops import commerce_pipeline_profile as profile


@pytest.mark.parametrize("kind", ["files", "snapshots"])
def test_inventory_includes_customer_history_in_the_gold_schema(kind):
    class Client:
        def query(self, sql):
            if "information_schema" in sql:
                tables = [{"table_schema": "gold", "table_name": "commerce_daily"}]
                if "table_schema='gold' AND table_name='dim_customers_scd2'" in sql:
                    tables.append({"table_schema": "gold", "table_name": "dim_customers_scd2"})
                return tables
            if "$files" in sql:
                return [{"files": 1, "bytes": 4850}]
            return [{"snapshot_id": 7}]

    collect = profile.file_inventory if kind == "files" else profile.snapshot_inventory
    inventory = collect(Client())
    assert inventory[-1] == {"table": "gold.dim_customers_scd2", **(
        {"files": 1, "bytes": 4850} if kind == "files" else {"snapshot_ids": [7]}
    )}


@pytest.mark.parametrize("mutation", [None, "files", "snapshots"])
def test_idle_replay_audits_history_even_when_other_tables_do_not_change(tmp_path, monkeypatch,
                                                                        mutation):
    executed = False

    class Client:
        def query(self, sql):
            if "information_schema" in sql:
                return [{"table_schema": "gold", "table_name": "dim_customers_scd2"}] if (
                    "dim_customers_scd2" in sql
                ) else []
            if "$files" in sql:
                return [{"files": 1, "bytes": 4850 + int(executed and mutation == "files")}]
            return [{"snapshot_id": 7 + int(executed and mutation == "snapshots")}]

    def execute(*args):
        nonlocal executed
        executed = True
        return {"status": "idle", "batch_id": None, "completed_stages": []}

    monkeypatch.setattr(profile, "execute", execute)
    ticks = iter([0, 1])
    if mutation:
        with pytest.raises(ValueError, match="changed files or snapshots"):
            profile.measure_idle_replay(Client(), [], tmp_path, "idle", clock=lambda: next(ticks))
    else:
        observation = profile.measure_idle_replay(Client(), [], tmp_path, "idle",
                                                  clock=lambda: next(ticks))
        assert observation["files"] == [{"table": "gold.dim_customers_scd2",
                                         "files": 1, "bytes": 4850}]


def test_history_only_catalog_is_not_fresh_for_a_profile(tmp_path, monkeypatch):
    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def query(self, sql):
            if "information_schema" in sql:
                return [{"table_schema": "gold", "table_name": "dim_customers_scd2"}] if (
                    "dim_customers_scd2" in sql
                ) else []
            return [{"files": 1, "bytes": 4850}]

    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "lakehouse-ops-scale-history-test")
    monkeypatch.setattr(profile, "TrinoClient", Client)
    monkeypatch.setattr(profile, "execute", lambda *args: pytest.fail("must not start a stage"))
    with pytest.raises(ValueError, match="fresh commerce catalog"):
        profile.profile(tmp_path, orders=[100, 1000], endpoint="unused", server="unused")
