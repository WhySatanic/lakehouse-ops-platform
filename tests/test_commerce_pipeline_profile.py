from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from lakehouse_ops import commerce_pipeline_profile as profile
from lakehouse_ops.commerce_pipeline import COMMERCE_STAGES, CURRENT_STAGE
from lakehouse_ops.trino import TrinoQueryResult, TrinoQueryStats


def test_memory_parser_ignores_other_stage_output():
    line = json.dumps({"app_name": "test", "kind": "spark_container_memory"})
    assert profile.memory_reports("Spark INFO\n" + line + "\n{}")[0]["app_name"] == "test"


def test_execute_retains_failed_process_logs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def run(command, **kwargs):
        assert command[-2:] == ["--s3-bucket", "lakehouse"]
        kwargs["stderr"].write("failed stage")
        raise subprocess.CalledProcessError(2, command)

    monkeypatch.setattr(profile.subprocess, "run", run)
    with pytest.raises(subprocess.CalledProcessError):
        profile.execute(["--s3-bucket", "lakehouse"], tmp_path, "failed")
    assert (tmp_path / "failed.stderr.log").read_text() == "failed stage"


@pytest.mark.parametrize("volumes", [[99, 100], [100, 99], [100]])
def test_invalid_volume_does_not_start_pipeline(tmp_path: Path, volumes):
    with pytest.raises(ValueError, match="ascending"):
        profile.profile(tmp_path, orders=volumes, endpoint="unused", server="unused")


def test_default_project_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("COMPOSE_PROJECT_NAME", raising=False)
    with pytest.raises(ValueError, match="dedicated"):
        profile.profile(tmp_path, orders=[100, 1000], endpoint="unused", server="unused")


@pytest.mark.parametrize(("failure", "with_references"), [(failure, enabled)
    for failure in [None, "memory", "idle", "existing", "second-run",
                                    "missing-current", "current-revenue", "wrong-sql-revenue",
                                    "stack-empty", "stack-partial-first", "stack-partial-second",
                    "warehouse-inventory", "last-run"] for enabled in (False, True)
] + [("reference-race", True), ("table-race", True)])
@pytest.mark.parametrize("volumes", [[100, 1000], [100, 100]])
@pytest.mark.parametrize(("customers", "batches", "revisions"), [
    (10, 2, None), (13, 3, None), (2, 4, [0, 1, 1, 0]),
])
def test_two_volume_evidence_or_retained_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure, volumes, with_references,
    customers, batches, revisions,
):
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "lakehouse-ops-scale-test")
    running = failure == "existing"
    selected_volume = 100
    selected_revenue = 0
    sampler_reports = 0
    selected_sample = 0
    reference_reads = 0

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def query(self, sql):
            if "information_schema" in sql:
                if failure == "table-race" and reference_reads:
                    return []
                return [{"table_schema": "gold", "table_name": "commerce_daily"}] if running else []
            if "$snapshots" in sql:
                return [{"snapshot_id": 100}]
            assert '"gold"."commerce_daily$files"' in sql
            return [{"files": 1, "bytes": 1024}]

        def query_with_stats(self, sql):
            if "dim_customers_scd2" in sql:
                periods = [("2026-02-01 00:00:00", None)]
                if selected_sample >= 2:
                    periods = [("2026-02-01 00:00:00", "2026-02-02 00:00:00"),
                               ("2026-02-02 00:00:00", None)]
                if selected_sample == 4:
                    periods[-1] = ("2026-02-02 00:00:00", "2026-02-04 00:00:00")
                    periods.append(("2026-02-04 00:00:00", None))
                rows = tuple({"valid_from": start, "valid_to": end, "is_current": end is None,
                              "rows": customers, "customers": customers,
                              "matching_customers": customers} for start, end in periods)
                if failure == "history" and selected_sample == 4:
                    rows[-1]["matching_customers"] -= 1
                return TrinoQueryResult("history", rows,
                                        TrinoQueryStats("FINISHED", 1, 1, 1, 1, 1, 1, 1, 0))
            revenue = selected_revenue
            row = {"revenue": revenue + int(failure == "wrong-sql-revenue")}
            if "commerce_current_daily" in sql:
                row["revenue"] += int(failure == "current-revenue")
            if "AS orders" in sql:
                row["orders"] = selected_volume
            return TrinoQueryResult("q1", (row,),
                                    TrinoQueryStats("FINISHED", 1, 1, 1, 1, 1, 1, 1, 0))

    def execute(args, output, label):
        nonlocal running, selected_volume, selected_revenue, selected_sample
        assert "--s3-bucket" in args
        if "landing" in label:
            return {}
        if "idle" in label:
            return {"status": "ready" if failure == "idle" else "idle",
                    "batch_id": None, "completed_stages": []}
        if failure == "second-run" and label == "sample-2":
            raise subprocess.CalledProcessError(2, args)
        if failure == "last-run" and label == f"sample-{batches}":
            raise subprocess.CalledProcessError(2, args)
        running = True
        selected_sample = int(label.split("-")[1])
        fixture = next((output / label).iterdir())
        manifest = json.loads((fixture / "manifest.json").read_text())
        batch = manifest["batch_id"]
        selected_volume = manifest["config"]["orders"]
        assert manifest["config"]["customers"] == customers
        assert manifest["config"].get("customer_revision", 0) == (
            revisions[selected_sample - 1] if revisions else 0
        )
        selected_revenue = sum(
            row["amount_cents"] for row in (
                json.loads(line) for line in (fixture / "payments.jsonl").read_text().splitlines()
            ) if type(row["amount_cents"]) is int and row["amount_cents"] >= 0
        )
        assert manifest["config"]["source_semantics"] == "full_snapshot"
        observations = [{"app_name": str(i), "kind": "spark_container_memory",
                         "status": "unavailable" if failure == "memory" else "observed"}
                        for i in range(8)]
        (output / f"{label}.stderr.log").write_text(
            "вњ“ Docker stage completed\n" + "\n".join(json.dumps(row) for row in observations),
            encoding="utf-8",
        )
        return {"status": "ready", "batch_id": batch,
                "completed_stages": list(COMMERCE_STAGES) + (
                    [] if failure == "missing-current" else [CURRENT_STAGE]
                )}

    monkeypatch.setattr(profile, "TrinoClient", Client)
    monkeypatch.setattr(profile, "execute", execute)
    class Sampler:
        def __init__(self, project):
            assert project == "lakehouse-ops-scale-test"

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def report(self):
            nonlocal sampler_reports
            sampler_reports += 1
            partial = failure in {"stack-empty", "stack-partial-first"} or (
                failure == "stack-partial-second" and sampler_reports == 2
            )
            empty = failure == "stack-empty"
            return {"status": "partial" if partial else "observed",
                    "peak_observed_bytes": None if empty else 100,
                    "samples": [] if empty else [{"used_bytes": 100}],
                    "errors": ["Docker sample unavailable"] if partial and not empty else []}

    monkeypatch.setattr(profile, "StackMemorySampler", Sampler)
    storage_client = object()
    storage_calls = []

    def inventory(client):
        assert client is storage_client
        storage_calls.append(True)
        if failure == "warehouse-inventory":
            raise ValueError("warehouse listing denied")
        return {"total_payload_bytes": len(storage_calls) * 8192}

    monkeypatch.setattr(profile, "warehouse_inventory", inventory)

    def references(client, tables):
        nonlocal reference_reads
        reference_reads += 1
        assert tables == [{"table_schema": "gold", "table_name": "commerce_daily"}]
        return [{"observation": reference_reads if failure == "reference-race" else 1}]

    def reconcile(storage, refs):
        assert storage == {"total_payload_bytes": len(storage_calls) * 8192}
        assert refs == [{"observation": 1}]
        return {"status": "reconciled"}

    monkeypatch.setattr(profile, "file_references", references)
    monkeypatch.setattr(profile, "reconcile_references", reconcile)
    if failure:
        with pytest.raises((ValueError, subprocess.CalledProcessError)):
            profile.profile(tmp_path, orders=volumes, endpoint="unused", server="unused",
                            warehouse_client=storage_client, retained_references=with_references,
                            customers=customers, batches=batches,
                            customer_revisions=revisions)
        assert json.loads((tmp_path / "profile.json").read_text())["status"] == "failed"
        if failure == "second-run":
            evidence = json.loads((tmp_path / "profile.json").read_text())
            assert len(evidence["samples"]) == 1
            assert evidence["samples"][0]["orders"] == 100
        if failure == "last-run":
            evidence = json.loads((tmp_path / "profile.json").read_text())
            assert len(evidence["samples"]) == batches - 1
        if failure == "history":
            assert len(json.loads((tmp_path / "profile.json").read_text())["samples"]) == 3
        if failure.startswith("stack-"):
            evidence = json.loads((tmp_path / "profile.json").read_text())
            count = 2 if failure == "stack-partial-second" else 1
            assert len(evidence["samples"]) == count
            assert evidence["samples"][-1]["stack_memory"]["status"] == "partial"
            assert evidence["samples"][-1]["run"]["status"] == "ready"
            assert evidence["samples"][-1]["idle_replay"] == "verified"
            assert json.loads((tmp_path / f"sample-{count}.stack-memory.json").read_text()) == (
                evidence["samples"][-1]["stack_memory"]
            )
            if count == 1:
                assert not (tmp_path / "sample-2").exists()
    else:
        result = profile.profile(tmp_path, orders=volumes, endpoint="unused", server="unused",
                                 warehouse_client=storage_client,
                                 retained_references=with_references, customers=customers,
                                 batches=batches, customer_revisions=revisions)
        assert result["status"] == "ready"
        assert result["warehouse_inventory"] == "enabled"
        if revisions:
            assert result["workload"]["customer_revisions"] == revisions
            assert [sample["customer_history"]["total_rows"] for sample in result["samples"]] == (
                [2, 4, 4, 6]
            )
        else:
            assert all("customer_history" not in sample for sample in result["samples"])
        assert result["retained_references"] == ("enabled" if with_references else "not_requested")
        assert reference_reads == (2 * batches if with_references else 0)
        for index, sample in enumerate(result["samples"], start=1):
            expected_storage = {"total_payload_bytes": index * 8192}
            if with_references:
                expected_storage["reference_accounting"] = {"status": "reconciled"}
            assert sample["warehouse_storage"] == expected_storage
            assert json.loads((tmp_path / f"sample-{index}.warehouse.json").read_text()) == (
                sample["warehouse_storage"]
            )
        assert [s["orders"] for s in result["samples"]] == (
            [volumes[0]] + [volumes[1]] * (batches - 1)
        )
        assert result["scope"] == (
            "sequential_snapshot_history_growth" if batches > 2 else (
                "same_volume_fresh_then_warm_catalog" if volumes[0] == volumes[1]
                else "two_volume_sequential_warm_stack"
            )
        )
        first, second = result["samples"][:2]
        assert first["batch_id"] != second["batch_id"]
        assert first["config"]["batch_at"] < second["config"]["batch_at"]
        assert [s["prior_completed_batches"] for s in result["samples"]] == list(range(batches))
        assert [s["catalog_state"] for s in result["samples"]] == [
            "fresh_commerce_catalog", "one_completed_prior_batch",
            *(["multiple_completed_prior_batches"] * (batches - 2)),
        ]
        if volumes[0] == volumes[1] and revisions is None:
            assert first["input_expected"] == second["input_expected"]
            fixtures = [next((tmp_path / label).iterdir()) for label in ("sample-1", "sample-2")]
            for table in ("customers", "products", "orders", "payments"):
                rows = [[json.loads(line) for line in (fixture / f"{table}.jsonl").read_text(
                    encoding="utf-8",
                ).splitlines()] for fixture in fixtures]
                for earlier, later in zip(*rows, strict=True):
                    for timestamp in ("event_at", "ingested_at", "paid_at"):
                        if timestamp in earlier:
                            assert datetime.fromisoformat(later.pop(timestamp)) - (
                                datetime.fromisoformat(earlier.pop(timestamp))
                            ) == timedelta(days=1)
                    assert earlier == later
        assert result["samples"][0]["idle_replay"] == "verified"
        assert result["samples"][0]["cumulative_current_files"][0]["bytes"] == 1024
        assert result["samples"][1]["query_costs"]["detail"]["rows"][0]["orders"] == volumes[1]
        assert result["samples"][0]["idle_replay_observation"]["status"] == "verified"


@pytest.mark.parametrize("failure", [None, "files", "snapshots", "compute", "batch"])
def test_idle_measurement_excludes_audit_time_and_rejects_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure,
):
    executed = False
    ticks = iter([10.0, 10.25])

    class Client:
        def query(self, sql):
            if "information_schema" in sql:
                return [{"table_schema": "gold", "table_name": "commerce_daily"}]
            if "$snapshots" in sql:
                return [{"snapshot_id": 2 if executed and failure == "snapshots" else 1}]
            return [{"files": 2 if executed and failure == "files" else 1, "bytes": 1024}]

    def execute(args, output, label):
        nonlocal executed
        executed = True
        return {"status": "idle", "batch_id": "wrong" if failure == "batch" else None,
                "completed_stages": ["spark"] if failure == "compute" else []}

    monkeypatch.setattr(profile, "execute", execute)
    if failure:
        with pytest.raises(ValueError, match="checkpoint replay"):
            profile.measure_idle_replay(Client(), [], tmp_path, "idle", clock=lambda: next(ticks))
    else:
        observation = profile.measure_idle_replay(
            Client(), [], tmp_path, "idle", clock=lambda: next(ticks),
        )
        assert observation["elapsed_seconds"] == 0.25
        assert observation["snapshots"] == [{"table": "gold.commerce_daily", "snapshot_ids": [1]}]


@pytest.mark.parametrize("failure", [None, "revenue", "orders", "volume", "empty", "duplicate"])
def test_detail_daily_costs_reconcile_before_evidence(failure):
    statements = []

    class Client:
        def query_with_stats(self, sql):
            statements.append(sql)
            row = {"orders": 100, "revenue": 12345}
            if failure == "volume":
                row["orders"] = 99
            if len(statements) == 2:
                if failure == "revenue":
                    row["revenue"] += 1
                if failure == "orders":
                    row["orders"] -= 1
            rows = () if failure == "empty" else (row,)
            if failure == "duplicate":
                rows = (row, row)
            return TrinoQueryResult(f"q{len(statements)}", rows,
                                    TrinoQueryStats("FINISHED", 2, 3, 4, 5, 6, 7, 8, 0))

    if failure:
        with pytest.raises(ValueError, match="reconcile"):
            profile.query_costs(Client(), "0123456789abcdef", 100)
    else:
        result = profile.query_costs(Client(), "0123456789abcdef", 100)
        assert result["detail"]["stats"]["processed_bytes"] == 6
        assert result["daily"]["query_id"] == "q2"
    assert "GROUP BY order_id" in statements[0]
    assert "LEFT JOIN" in statements[0]
    assert all("source_batch_id='0123456789abcdef'" in sql for sql in statements)


def test_query_costs_rejects_unbound_batch_before_query():
    with pytest.raises(ValueError, match="batch ID"):
        profile.query_costs(None, "' OR true --", 100)


def test_wrong_final_customer_state_retains_only_verified_samples(tmp_path, monkeypatch):
    test_two_volume_evidence_or_retained_failure(
        tmp_path, monkeypatch, "history", [100, 100], True, 2, 4, [0, 1, 1, 0],
    )


def test_reference_accounting_requires_inventory_before_creating_output(tmp_path):
    output = tmp_path / "new"
    with pytest.raises(ValueError, match="require warehouse inventory"):
        profile.profile(output, orders=[100, 100], endpoint="unused", server="unused",
                        retained_references=True)
    assert not output.exists()
