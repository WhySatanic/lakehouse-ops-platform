from __future__ import annotations

import os
import subprocess

import pytest

from lakehouse_ops.commerce_pipeline import run_compose_stage


@pytest.mark.parametrize("service", ["bronze-input-sync", "spark-commerce-bronze"])
def test_selected_sync_is_scoped_to_runner_child_environment(monkeypatch, service):
    monkeypatch.setenv("COMMERCE_SYNC_SCOPE", "all")
    commands = []

    def run(command, **kwargs):
        commands.append(kwargs["env"])
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr("lakehouse_ops.commerce_pipeline.subprocess.run", run)
    run_compose_stage(service, {"batch_id": "a" * 16, "manifest_sha256": "b" * 64}, bucket="test")
    assert commands[0]["COMMERCE_SYNC_SCOPE"] == (
        "selected" if service == "bronze-input-sync" else "all"
    )
    assert commands[0]["COMMERCE_BATCH_ID"] == "a" * 16
    assert os.environ["COMMERCE_SYNC_SCOPE"] == "all"
