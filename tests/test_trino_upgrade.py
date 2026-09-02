from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops.trino import TrinoQueryResult, TrinoQueryStats
from lakehouse_ops.trino_upgrade import (
    EXPECTED_WORKLOAD,
    UpgradeRehearsalError,
    load_upgrade_plan,
    run_upgrade_rehearsal,
    validate_upgrade_report,
    write_upgrade_report,
)


class FakeClient:
    def __init__(self, version: str, *, drift: bool = False) -> None:
        self.version = version
        self.drift = drift
        self.closed = False

    def query(self, sql: str) -> list[dict[str, Any]]:
        if "system.runtime.nodes" in sql:
            return [
                {
                    "node_id": "lakehouse-coordinator",
                    "node_version": self.version,
                    "coordinator": True,
                    "state": "active",
                },
                {
                    "node_id": "lakehouse-worker-1",
                    "node_version": self.version,
                    "coordinator": False,
                    "state": "active",
                },
                {
                    "node_id": "lakehouse-worker-2",
                    "node_version": self.version,
                    "coordinator": False,
                    "state": "active",
                },
            ]
        if "weather_hourly$files" in sql:
            return [
                {
                    "data_file_count": 2,
                    "record_count": 2,
                    "total_size_bytes": 2048,
                }
            ]
        if "$snapshots" in sql:
            return [{"snapshot_id": "42"}]
        raise AssertionError(f"unexpected query: {sql}")

    def query_with_stats(self, sql: str) -> TrinoQueryResult:
        workload = dict(EXPECTED_WORKLOAD)
        if self.drift:
            workload["silver_rows"] = 3
        return TrinoQueryResult(
            query_id=f"query-{self.version}",
            rows=(workload,),
            stats=TrinoQueryStats(
                state="FINISHED",
                elapsed_time_ms=20,
                wall_time_ms=10,
                cpu_time_ms=5,
                processed_rows=9,
                processed_bytes=100,
                physical_input_bytes=50,
                peak_memory_bytes=100,
                spilled_bytes=0,
            ),
        )

    def close(self) -> None:
        self.closed = True


def write_plan(path: Path, *, source: str = "482", target: str = "483") -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "source": {
                    "version": source,
                    "image": f"trinodb/trino:{source}",
                },
                "target": {
                    "version": target,
                    "image": f"trinodb/trino:{target}",
                },
                "release_notes": (
                    f"https://trino.io/docs/current/release/release-{target}.html"
                ),
            }
        ),
        encoding="utf-8",
    )


def transition_evidence(spec: dict[str, str], call: int = 1) -> dict[str, Any]:
    return {
        "active_nodes": 3,
        "coordinator_id": f"coordinator-{spec['version']}-{call}",
        "version": spec["version"],
        "image": spec["image"],
        "container_image_id": "sha256:" + "a" * 64,
    }


def test_load_upgrade_plan_pins_adjacent_versions(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    write_plan(path)

    plan = load_upgrade_plan(path)

    assert plan["source"]["version"] == "482"
    assert plan["target"]["version"] == "483"
    assert len(plan["plan_sha256"]) == 64


def test_load_upgrade_plan_accepts_digest_pinned_images(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    write_plan(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["source"]["image"] += "@sha256:" + "a" * 64
    payload["target"]["image"] += "@sha256:" + "b" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")

    plan = load_upgrade_plan(path)

    assert plan["source"]["image"].endswith("a" * 64)
    assert plan["target"]["image"].endswith("b" * 64)


@pytest.mark.parametrize(
    "source, target, message",
    [
        ("481", "483", "next Trino release"),
        ("482", "not-a-version", "numeric"),
    ],
)
def test_load_upgrade_plan_rejects_invalid_versions(
    tmp_path: Path, source: str, target: str, message: str
) -> None:
    path = tmp_path / "plan.json"
    write_plan(path, source=source, target=target)

    with pytest.raises(UpgradeRehearsalError, match=message):
        load_upgrade_plan(path)


def test_load_upgrade_plan_rejects_invalid_file(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    path.write_text("not json", encoding="utf-8")

    with pytest.raises(UpgradeRehearsalError, match="cannot read"):
        load_upgrade_plan(path)


def test_write_upgrade_report_creates_parent_directory(tmp_path: Path) -> None:
    path = tmp_path / "evidence" / "upgrade.json"
    report = {"status": "ready"}

    write_upgrade_report(path, report)

    assert json.loads(path.read_text(encoding="utf-8")) == report


def test_run_upgrade_rehearsal_proves_upgrade_and_rollback(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    write_plan(path)
    plan = load_upgrade_plan(path)
    current = {"version": "482"}
    switches: list[str] = []

    def switch(spec: dict[str, str]) -> dict[str, Any]:
        current["version"] = spec["version"]
        switches.append(spec["version"])
        return transition_evidence(spec, len(switches))

    report = run_upgrade_rehearsal(
        lambda: FakeClient(current["version"]),
        switch,
        transition_evidence(plan["source"], 0),
        plan,
        clock=lambda: datetime(2026, 8, 25, 9, 0, tzinfo=UTC),
    )

    assert switches == ["483", "482", "483"]
    assert [phase["phase"] for phase in report["phases"]] == [
        "baseline",
        "upgraded",
        "rolled_back",
        "restored",
    ]
    assert len({phase["data_fingerprint"] for phase in report["phases"]}) == 1
    assert report["compatibility"]["rollback"] == "passed"
    assert report["collected_at"] == "2026-08-25T09:00:00+00:00"


def test_run_upgrade_rehearsal_accepts_digest_pinned_phase_images(
    tmp_path: Path,
) -> None:
    path = tmp_path / "plan.json"
    write_plan(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["source"]["image"] += "@sha256:" + "a" * 64
    payload["target"]["image"] += "@sha256:" + "b" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")
    plan = load_upgrade_plan(path)
    current = {"version": "482"}

    def switch(spec: dict[str, str]) -> dict[str, Any]:
        current["version"] = spec["version"]
        return transition_evidence(spec)

    report = run_upgrade_rehearsal(
        lambda: FakeClient(current["version"]),
        switch,
        transition_evidence(plan["source"], 0),
        plan,
    )

    assert report["phases"][0]["image"] == plan["source"]["image"]
    assert report["phases"][1]["image"] == plan["target"]["image"]


def test_run_upgrade_rehearsal_restores_target_after_failure(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    write_plan(path)
    plan = load_upgrade_plan(path)
    current = {"version": "482"}
    switches: list[str] = []
    clients = 0

    def switch(spec: dict[str, str]) -> dict[str, Any]:
        current["version"] = spec["version"]
        switches.append(spec["version"])
        return transition_evidence(spec, len(switches))

    def client_factory() -> FakeClient:
        nonlocal clients
        clients += 1
        return FakeClient(current["version"], drift=clients == 3)

    with pytest.raises(UpgradeRehearsalError, match="unexpected data"):
        run_upgrade_rehearsal(
            client_factory,
            switch,
            transition_evidence(plan["source"], 0),
            plan,
        )

    assert switches == ["483", "482", "483"]
    assert current["version"] == "483"


def test_run_upgrade_rehearsal_recovers_from_failed_rollback(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    write_plan(path)
    plan = load_upgrade_plan(path)
    current = {"version": "482"}
    switches: list[str] = []

    def switch(spec: dict[str, str]) -> dict[str, Any]:
        switches.append(spec["version"])
        if switches == ["483", "482"]:
            raise RuntimeError("rollback container failed")
        current["version"] = spec["version"]
        return transition_evidence(spec, len(switches))

    with pytest.raises(RuntimeError, match="rollback container failed"):
        run_upgrade_rehearsal(
            lambda: FakeClient(current["version"]),
            switch,
            transition_evidence(plan["source"], 0),
            plan,
        )

    assert switches == ["483", "482", "483"]
    assert current["version"] == "483"


def test_validate_upgrade_report_rejects_data_drift(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    write_plan(path)
    plan = load_upgrade_plan(path)
    current = {"version": "482"}

    def switch(spec: dict[str, str]) -> dict[str, Any]:
        current["version"] = spec["version"]
        return transition_evidence(spec)

    report = run_upgrade_rehearsal(
        lambda: FakeClient(current["version"]),
        switch,
        transition_evidence(plan["source"], 0),
        plan,
    )
    report["phases"][2]["data_fingerprint"] = "0" * 64

    with pytest.raises(UpgradeRehearsalError, match="fingerprint"):
        validate_upgrade_report(report, plan)
