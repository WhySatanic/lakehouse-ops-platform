from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path

import pytest

from lakehouse_ops.commerce_pipeline_profile import customer_history_observation, workload_configs
from lakehouse_ops.trino import TrinoClient


def literal_rows(final_revision):
    # Independent literal oracle: Feb 3 repeats Feb 2, so there is no Feb 3 period.
    return [
        ("C00000001", "Customer 00000001", "2026-02-01", "2026-02-02", False),
        ("C00000002", "Customer 00000002", "2026-02-01", "2026-02-02", False),
        ("C00000001", "Customer 00000001 revision 1", "2026-02-02", "2026-02-04", False),
        ("C00000002", "Customer 00000002 revision 1", "2026-02-02", "2026-02-04", False),
        ("C00000001", "Customer 00000001" + (" revision 2" if final_revision else ""),
         "2026-02-04", None, True),
        ("C00000002", "Customer 00000002" + (" revision 2" if final_revision else ""),
         "2026-02-04", None, True),
    ]


class QueryInput:
    def __init__(self, client, evidence, rows=None):
        self.client, self.evidence, self.rows = client, evidence, rows

    def query_with_stats(self, sql):
        if self.rows is not None:
            values = []
            for customer, name, start, end, current in self.rows:
                stop = f"TIMESTAMP '{end} 00:00:00'" if end else "CAST(NULL AS timestamp)"
                values.append(
                    f"('{customer}', '{name}', TIMESTAMP '{start} 00:00:00', "
                    f"{stop}, {'true' if current else 'false'})",
                )
            source = "FROM lakehouse.gold.dim_customers_scd2"
            assert sql.count(source) == 1
            sql = (
                "WITH history(customer_id, full_name, valid_from, valid_to, is_current) AS (VALUES "
                + ", ".join(values) + ") " + sql.replace(source, "FROM history")
            )
        result = self.client.query_with_stats(sql)
        self.evidence.append({"sql": sql, **asdict(result)})
        return result


def exercise(client, report):
    configs = workload_configs([100, 100], customers=2, batches=4)
    cases = report["cases"] = []
    for final_revision, fault in [(2, None), (0, None), *(
        (2, fault) for fault in ("names", "boundary", "duplicate", "missing", "current")
    )]:
        rows = literal_rows(final_revision)
        if fault == "names":
            rows[0] = (*rows[0][:1], "Wrong customer state", *rows[0][2:])
        elif fault == "boundary":
            rows[2] = (*rows[2][:3], "2026-02-03", rows[2][4])
        elif fault == "duplicate":
            rows.append(rows[0])
        elif fault == "missing":
            rows.pop(0)
        elif fault == "current":
            rows[0] = (*rows[0][:4], True)
        case = {"final_revision": final_revision, "fault": fault, "queries": []}
        cases.append(case)
        query_input = QueryInput(client, case["queries"], rows)
        if fault:
            with pytest.raises(ValueError, match="customer history"):
                customer_history_observation(query_input, configs, [0, 1, 1, final_revision])
            case["outcome"] = "rejected"
        else:
            case["observation"] = customer_history_observation(
                query_input, configs, [0, 1, 1, final_revision],
            )
            assert case["observation"]["total_rows"] == 6
            assert case["observation"]["current_rows"] == 2
            case["outcome"] = "verified"
    # Also execute the unchanged production FROM against the real CI Iceberg table.
    # Its 40-customer seed is deliberately not the two-customer literal workload.
    report["catalog_probe"] = []
    with pytest.raises(ValueError, match="customer history"):
        customer_history_observation(QueryInput(client, report["catalog_probe"]),
                                     configs, [0, 1, 1, 2])
    assert report["catalog_probe"][0]["rows"]
    report["status"] = "ready"


@pytest.mark.skipif(os.getenv("LAKEOPS_RUN_REVISION_TRINO") != "1",
                    reason="requires the existing CI Trino and 40-customer Iceberg seed")
def test_revision_sql_on_real_trino():
    output = Path("artifacts/commerce-revision-trino.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {"status": "failed", "scope": "literal_sql_execution_not_spark_growth_measurement",
              "source_sha": os.getenv("GITHUB_SHA")}
    try:
        with TrinoClient(server="http://localhost:8080", user="lakehouse-ci",
                         query_timeout=30) as client:
            exercise(client, report)
    finally:
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")
