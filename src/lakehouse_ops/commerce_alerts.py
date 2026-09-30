from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import httpx

from lakehouse_ops.ingestion.commerce_batches import CommerceBatchPlanner


class CommerceAlertError(ValueError):
    pass


def notify_commerce_freshness(
    planner: CommerceBatchPlanner,
    *,
    server: str,
    instance: str,
    source_max_age_seconds: int = 900,
    backlog_max_age_seconds: int = 900,
    valid_seconds: int = 180,
    now: datetime | None = None,
) -> dict[str, Any]:
    validate_commerce_alert_options(
        server=server, instance=instance,
        source_max_age_seconds=source_max_age_seconds,
        backlog_max_age_seconds=backlog_max_age_seconds,
        valid_seconds=valid_seconds,
    )
    observed_at = now or datetime.now(UTC)
    if observed_at.tzinfo is None:
        raise CommerceAlertError("observation time must include a timezone")
    source = planner.check_source_freshness(
        max_age_seconds=source_max_age_seconds, now=observed_at,
    )
    backlog = planner.check_backlog_freshness(
        max_age_seconds=backlog_max_age_seconds, now=observed_at,
    )
    alerts = []
    for report, name, component, action in (
        (source, "LakehouseCommerceSourceStale", "ingestion",
         "Check source generation and MinIO landing; do not use fixture event time as freshness."),
        (backlog, "LakehouseCommerceBacklogStale", "processing",
         "Inspect the oldest pending batch and Spark/Trino failures; "
         "verify gold before checkpointing."),
    ):
        stale = report["status"] == "stale"
        ends_at = observed_at + timedelta(seconds=valid_seconds if stale else 0)
        alerts.append({
            "labels": {
                "alertname": name, "instance": instance,
                "component": component, "severity": "warning", "source": "commerce",
            },
            "annotations": {
                "summary": f"Commerce {component} freshness check",
                "description": f"{action} Observation: {report}",
                "runbook_url": "https://github.com/WhySatanic/lakehouse-ops-platform/"
                "blob/main/docs/runbooks/commerce-training-fixture.md",
            },
            "startsAt": _timestamp(observed_at - timedelta(seconds=1)),
            "endsAt": _timestamp(ends_at),
        })
    try:
        with httpx.Client(timeout=10) as client:
            response = client.post(f"{server.rstrip('/')}/api/v2/alerts", json=alerts)
            response.raise_for_status()
    except httpx.HTTPError as error:
        raise CommerceAlertError("Alertmanager did not accept commerce freshness alerts") from error
    return {
        "status": "stale" if any(r["status"] == "stale" for r in (source, backlog)) else "ready",
        "notification": "accepted",
        "instance": instance,
        "alerts_sent": len(alerts),
        "source": source,
        "backlog": backlog,
    }


def validate_commerce_alert_options(
    *, server: str, instance: str, source_max_age_seconds: int,
    backlog_max_age_seconds: int, valid_seconds: int,
) -> None:
    try:
        urlsplit(server)
        url = httpx.URL(server)
    except (httpx.InvalidURL, ValueError) as error:
        raise CommerceAlertError("invalid Alertmanager URL") from error
    if (
        url.scheme not in {"http", "https"}
        or not url.host
        or url.userinfo
        or url.query
        or url.fragment
    ):
        raise CommerceAlertError("Alertmanager URL must be HTTP(S) without credentials or query")
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", instance):
        raise CommerceAlertError("instance must be a stable identifier of 1 to 128 characters")
    if type(valid_seconds) is not int or not 30 <= valid_seconds <= 86400:
        raise CommerceAlertError("valid_seconds must be between 30 and 86400")
    for name, value in (
        ("source_max_age_seconds", source_max_age_seconds),
        ("backlog_max_age_seconds", backlog_max_age_seconds),
    ):
        if type(value) is not int or value < 1:
            raise CommerceAlertError(f"{name} must be a positive integer")


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
