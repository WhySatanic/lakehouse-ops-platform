from __future__ import annotations

import pytest

from lakehouse_ops import commerce_pipeline_profile as profile
from lakehouse_ops.trino import TrinoQueryResult, TrinoQueryStats


@pytest.mark.parametrize("revisions", [[], [0], [0, True], [0, -1], [0, 10001], [0, 1.5]])
def test_invalid_revision_schedule_creates_no_evidence(tmp_path, revisions):
    with pytest.raises(ValueError, match="revision"):
        profile.profile(tmp_path / "evidence", orders=[100, 100], endpoint="unused",
                        server="unused", customer_revisions=revisions)
    assert not (tmp_path / "evidence").exists()


@pytest.mark.parametrize("revisions", [[0, 1, 1, 2], [0, 1, 1, 0]])
@pytest.mark.parametrize("fault", [None, "missing", "extra", "rows", "customers", "names",
                                  "current", "boundary"])
def test_literal_customer_period_oracle(revisions, fault):
    configs = profile.workload_configs([100, 100], customers=10000, batches=4)
    rows = [
        {"valid_from": "2026-02-01 00:00:00.000000", "valid_to": "2026-02-02 00:00:00",
         "is_current": False, "rows": 10000, "customers": 10000, "matching_customers": 10000},
        {"valid_from": "2026-02-02 00:00:00", "valid_to": "2026-02-04 00:00:00",
         "is_current": False, "rows": 10000, "customers": 10000, "matching_customers": 10000},
        {"valid_from": "2026-02-04 00:00:00", "valid_to": None,
         "is_current": True, "rows": 10000, "customers": 10000, "matching_customers": 10000},
    ]
    if fault == "missing":
        rows.pop(0)
    elif fault == "extra":
        rows.append(dict(rows[0]))
    elif fault in {"rows", "customers"}:
        rows[0][fault] -= 1
    elif fault == "names":
        rows[0]["matching_customers"] -= 1
    elif fault == "current":
        rows[0]["is_current"] = True
    elif fault == "boundary":
        rows[1]["valid_to"] = "2026-02-03 00:00:00"

    class Client:
        def query_with_stats(self, sql):
            assert "lakehouse.gold.dim_customers_scd2" in sql
            assert "count(DISTINCT customer_id)" in sql
            assert "BETWEEN 1 AND 10000" in sql
            assert "^C[0-9]{8}$" in sql
            assert "LIMIT 101" in sql
            assert "TIMESTAMP '2026-02-03" not in sql
            assert "WHEN valid_from = TIMESTAMP '2026-02-04 00:00:00' THEN " + (
                "' revision 2'" if revisions[-1] == 2 else "''"
            ) in sql
            return TrinoQueryResult("history-q", tuple(rows),
                                    TrinoQueryStats("FINISHED", 1, 1, 1, 1, 1, 1, 1, 0))

    if fault:
        with pytest.raises(ValueError, match="customer history"):
            profile.customer_history_observation(Client(), configs, revisions)
    else:
        evidence = profile.customer_history_observation(Client(), configs, revisions)
        assert evidence["status"] == "verified"
        assert evidence["total_rows"] == 30000
        assert evidence["current_rows"] == 10000
        assert evidence["query_id"] == "history-q"
        assert [row["revision"] for row in evidence["expected_periods"]] == [0, 1, revisions[-1]]


def test_single_unchanged_period_includes_missing_email_customer():
    configs = profile.workload_configs([100, 100], customers=2, batches=2)
    class Client:
        def query_with_stats(self, sql):
            assert "email IS NOT NULL" not in sql
            return TrinoQueryResult("one", ({
                "valid_from": "2026-02-01 00:00:00", "valid_to": None,
                "is_current": True, "rows": 2, "customers": 2, "matching_customers": 2,
            },), TrinoQueryStats("FINISHED", 1, 1, 1, 1, 1, 1, 1, 0))
    assert profile.customer_history_observation(Client(), configs, [0, 0])["total_rows"] == 2


@pytest.mark.parametrize("start,end", [
    ("2026-02-01 00:00:00.000000 UTC", "2026-02-02 00:00:00.000000 UTC"),
    ("2026-02-01T00:00:00+00:00", "2026-02-02T00:00:00+00:00"),
    ("2026-02-01T03:00:00+03:00", "2026-02-02T03:00:00+03:00"),
    ("2026-01-31T19:00:00-05:00", "2026-02-01T19:00:00-05:00"),
])
def test_trino_zoned_periods_compare_instants_in_utc(start, end):
    class Client:
        def query_with_stats(self, sql):
            rows = (
                {"valid_from": start, "valid_to": end, "is_current": False,
                 "rows": 2, "customers": 2, "matching_customers": 2},
                {"valid_from": end, "valid_to": None, "is_current": True,
                 "rows": 2, "customers": 2, "matching_customers": 2},
            )
            return TrinoQueryResult("zoned", rows,
                                    TrinoQueryStats("FINISHED", 1, 1, 1, 1, 1, 1, 1, 0))
    configs = profile.workload_configs([100, 100], customers=2, batches=2)
    result = profile.customer_history_observation(Client(), configs, [0, 1])
    assert result["status"] == "verified" and result["total_rows"] == 4
    assert result["rows"][0]["valid_from"] == start


def test_offset_with_same_clock_is_not_the_expected_utc_instant():
    class Client:
        def query_with_stats(self, sql):
            return TrinoQueryResult("shifted", ({
                "valid_from": "2026-02-01T00:00:00+03:00", "valid_to": None,
                "is_current": True, "rows": 2, "customers": 2, "matching_customers": 2,
            },), TrinoQueryStats("FINISHED", 1, 1, 1, 1, 1, 1, 1, 0))
    with pytest.raises(ValueError, match="customer history periods"):
        profile.customer_history_observation(
            Client(), profile.workload_configs([100, 100], customers=2), [0, 0],
        )
