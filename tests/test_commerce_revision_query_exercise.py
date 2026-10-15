from __future__ import annotations

import json
import runpy
from dataclasses import asdict
from pathlib import Path

import pytest

from lakehouse_ops.trino import TrinoQueryResult, TrinoQueryStats


def module():
    return runpy.run_path(str(Path(__file__).parent / "integration" /
                               "test_commerce_revision_trino.py"))


@pytest.mark.parametrize("literal", [False, True])
def test_query_fixture_keeps_aggregate_and_raw_engine_evidence(literal):
    exercise = module()
    sql = "SELECT count(*) FROM lakehouse.gold.dim_customers_scd2 GROUP BY is_current"
    result = TrinoQueryResult("q1", ({"rows": 6},),
                              TrinoQueryStats("FINISHED", 1, 1, 1, 1, 1, 1, 1, 0))
    class Client:
        def query_with_stats(self, actual):
            if literal:
                assert actual.startswith("WITH history(")
                assert "FROM lakehouse.gold.dim_customers_scd2" not in actual
                assert "SELECT count(*) FROM history GROUP BY is_current" in actual
                assert "TIMESTAMP '2026-02-04 00:00:00'" in actual
                assert "CAST(NULL AS timestamp)" in actual
                assert "Customer 00000002 revision 2" in actual
                assert "INSERT" not in actual and "CREATE" not in actual
            else:
                assert actual == sql
            return result
    queries = []
    wrapper = exercise["QueryInput"](
        Client(), queries, exercise["literal_rows"](2) if literal else None,
    )
    assert wrapper.query_with_stats(sql) is result
    assert len(queries) == 1
    assert queries[0]["rows"] == result.rows
    assert queries[0]["stats"] == asdict(result.stats)
    assert queries[0]["query_id"] == "q1"


def test_trino_setup_failure_preserves_failed_json(tmp_path, monkeypatch):
    exercise = module()
    def unavailable(*args):
        raise RuntimeError("Trino unavailable")
    monkeypatch.setattr(exercise["TrinoClient"], "__enter__", unavailable)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(RuntimeError, match="unavailable"):
        exercise["test_revision_sql_on_real_trino"]()
    report = json.loads((tmp_path / "artifacts/commerce-revision-trino.json").read_text())
    assert report["status"] == "failed"
    assert report["scope"] == "literal_sql_execution_not_spark_growth_measurement"


def test_ci_executes_sql_after_real_history_seed_and_always_uploads_evidence():
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    job = workflow.split("  lakehouse-integration:\n", 1)[1].split(
        "  release-readiness:\n", 1,
    )[0]
    gate = job.split("      - name: Verify customer revision SQL against Trino\n", 1)[1].split(
        "      - name:", 1,
    )[0]
    assert "LAKEOPS_RUN_REVISION_TRINO=1" in gate
    assert "test_commerce_revision_trino.py" in gate
    assert job.index("Verify commerce customer SCD2 contract") < job.index(
        "Verify customer revision SQL against Trino",
    )
    upload = job.split("name: Upload core and recovery evidence", 1)[1].split(
        "      - name:", 1,
    )[0]
    assert "if: always()" in upload
    assert "artifacts/commerce-revision-trino.json" in upload
