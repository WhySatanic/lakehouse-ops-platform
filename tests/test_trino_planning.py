from dataclasses import asdict

import httpx
import pytest

from lakehouse_ops.trino import TrinoClient, TrinoProtocolError


def stats(**extra):
    return dict(state="FINISHED", elapsedTimeMillis=21, wallTimeMillis=12,
                cpuTimeMillis=7, processedRows=1, processedBytes=8,
                physicalInputBytes=4, peakMemoryBytes=16, spilledBytes=0, **extra)


def client_for(final, initial=None):
    def handler(request):
        if initial is not None and request.method == "POST":
            return httpx.Response(200, json={"id": "planning-query", "stats": initial,
                "nextUri": "http://internal:8080/v1/statement/planning-query/1"})
        return httpx.Response(200, json={"id": "planning-query", "stats": final,
            "columns": [{"name": "answer"}], "data": [[1]]})
    return TrinoClient("http://localhost:8080", client=httpx.Client(
        transport=httpx.MockTransport(handler)))


@pytest.mark.parametrize("value", [0, 17])
def test_planning_observation_retains_final_metric_and_legacy_result(value):
    with client_for(stats(planningTimeMillis=value)) as client:
        observed = client.query_with_planning("SELECT 1")
        legacy = client.query_with_stats("SELECT 1")
    assert observed.planning_time_ms == value
    assert observed.result == legacy
    assert observed.result.rows == ({"answer": 1},)
    assert "planning_time_ms" not in asdict(legacy.stats)
    assert set(asdict(legacy)) == {"query_id", "rows", "stats"}


def test_planning_uses_terminal_page_not_earlier_page():
    with client_for(stats(planningTimeMillis=17), stats(planningTimeMillis=1)) as client:
        assert client.query_with_planning("SELECT 1").planning_time_ms == 17


def test_missing_final_planning_is_unknown_even_when_previous_page_has_value():
    with client_for(stats(), stats(planningTimeMillis=1)) as client:
        assert client.query_with_planning("SELECT 1").planning_time_ms is None


@pytest.mark.parametrize("value", [None, True, False, -1, 1.5, "17"])
def test_malformed_planning_is_rejected_only_by_explicit_observation(value):
    with client_for(stats(planningTimeMillis=value)) as client:
        with pytest.raises(TrinoProtocolError, match="planningTimeMillis"):
            client.query_with_planning("SELECT 1")
        assert client.query_with_stats("SELECT 1").rows == ({"answer": 1},)


def test_observation_still_rejects_unfinished_query():
    final = stats(planningTimeMillis=0)
    final["state"] = "RUNNING"
    with client_for(final) as client, pytest.raises(TrinoProtocolError, match="not FINISHED"):
        client.query_with_planning("SELECT 1")
