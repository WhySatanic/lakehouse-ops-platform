from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from lakehouse_ops import commerce_alerts
from lakehouse_ops.ingestion.commerce_batches import CommerceBatchError

NOW = datetime(2026, 9, 28, 2, 0, tzinfo=UTC)


class Planner:
    def __init__(self, source: str = "stale", backlog: str = "stale") -> None:
        self.source = source
        self.backlog = backlog
        self.observations: list[tuple[int, datetime]] = []

    def check_source_freshness(self, *, max_age_seconds: int, now: datetime) -> dict[str, Any]:
        self.observations.append((max_age_seconds, now))
        return {"status": self.source, "latest_batch_id": "a" * 16, "age_seconds": 1000}

    def check_backlog_freshness(self, *, max_age_seconds: int, now: datetime) -> dict[str, Any]:
        self.observations.append((max_age_seconds, now))
        return {"status": self.backlog, "oldest_pending_batch_id": "b" * 16, "pending_batches": 2}


@pytest.mark.parametrize("statuses", [("stale", "stale"), ("ready", "stale"), ("ready", "ready")])
def test_delivers_stable_actionable_alerts_and_explicit_recovery(
    monkeypatch: pytest.MonkeyPatch, statuses: tuple[str, str],
) -> None:
    received = []
    planner = Planner(*statuses)

    def accept(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "http://alertmanager:9093/api/v2/alerts"
        assert request.method == "POST"
        assert planner.observations == [(60, NOW), (120, NOW)]
        received.extend(json.loads(request.content))
        return httpx.Response(200)

    original = httpx.Client
    monkeypatch.setattr(commerce_alerts.httpx, "Client", lambda **kwargs: original(
        transport=httpx.MockTransport(accept), **kwargs,
    ))
    report = commerce_alerts.notify_commerce_freshness(
        planner, server="http://alertmanager:9093/", instance="commerce-test",
        source_max_age_seconds=60, backlog_max_age_seconds=120, valid_seconds=300, now=NOW,
    )
    assert report["notification"] == "accepted"
    assert report["alerts_sent"] == 2
    assert report["status"] == ("stale" if "stale" in statuses else "ready")
    assert [a["labels"]["alertname"] for a in received] == [
        "LakehouseCommerceSourceStale", "LakehouseCommerceBacklogStale",
    ]
    for alert, status in zip(received, statuses, strict=True):
        assert alert["labels"]["instance"] == "commerce-test"
        assert "batch_id" not in alert["labels"]
        assert "Observation" in alert["annotations"]["description"]
        assert alert["annotations"]["runbook_url"].endswith("commerce-training-fixture.md")
        ends_at = datetime.fromisoformat(alert["endsAt"])
        assert ends_at == NOW + timedelta(seconds=300 if status == "stale" else 0)
        assert datetime.fromisoformat(alert["startsAt"]) < ends_at


@pytest.mark.parametrize("failure", ["timeout", "http"])
def test_delivery_failure_is_not_reported_as_accepted(
    monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        if failure == "timeout":
            raise httpx.ReadTimeout("failed", request=request)
        return httpx.Response(503)

    original = httpx.Client
    monkeypatch.setattr(commerce_alerts.httpx, "Client", lambda **kwargs: original(
        transport=httpx.MockTransport(fail), **kwargs,
    ))
    with pytest.raises(commerce_alerts.CommerceAlertError, match="did not accept"):
        commerce_alerts.notify_commerce_freshness(
            Planner(), server="http://alertmanager:9093", instance="commerce-test", now=NOW,
        )


@pytest.mark.parametrize("options", [
    {"server": "ftp://host"}, {"server": "http://user:secret@host"},
    {"server": "http://host?secret=value"}, {"server": "http://host#fragment"},
    {"server": "http://host:invalid"}, {"server": "http://[invalid"},
    {"instance": ""}, {"instance": "invalid instance"},
    {"valid_seconds": 29}, {"valid_seconds": 86401}, {"valid_seconds": True},
    {"source_max_age_seconds": 0}, {"backlog_max_age_seconds": 0},
    {"now": datetime(2026, 9, 28)},
])
def test_invalid_configuration_fails_before_observing_or_sending(options: dict[str, Any]) -> None:
    planner = Planner()
    with pytest.raises(commerce_alerts.CommerceAlertError):
        commerce_alerts.notify_commerce_freshness(planner, **{
            "server": "http://localhost:9093", "instance": "commerce-test", "now": NOW, **options,
        })
    assert planner.observations == []


def test_checkpoint_error_never_sends_a_healthy_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    planner = Planner("ready", "ready")

    def fail(**kwargs: Any) -> dict[str, Any]:
        raise CommerceBatchError("checkpoint is unreadable")

    def forbidden(**kwargs: Any) -> None:
        pytest.fail("must not send partial observations")

    monkeypatch.setattr(planner, "check_backlog_freshness", fail)
    monkeypatch.setattr(commerce_alerts.httpx, "Client", forbidden)
    with pytest.raises(CommerceBatchError, match="checkpoint"):
        commerce_alerts.notify_commerce_freshness(
            planner, server="http://localhost:9093", instance="commerce-test", now=NOW,
        )
