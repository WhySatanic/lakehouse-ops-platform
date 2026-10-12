"""Opt-in real Docker timeout and scoped cleanup acceptance."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

from lakehouse_ops import commerce_pipeline as pipeline


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_DOCKER_TIMEOUT_SMOKE") != "1", reason="requires local Docker",
)
def test_real_compose_timeout_removes_owned_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    compose = tmp_path / "compose.yaml"
    compose.write_text(
        "services:\n  bronze-input-sync:\n"
        "    image: lakehouse-ops/spark-iceberg:3.5.9-1.11.0\n"
        "    network_mode: none\n    mem_limit: 128m\n    entrypoint: python3\n"
        "    command: [-c, 'import time; time.sleep(60)']\n",
        encoding="utf-8",
    )
    real_run = subprocess.run
    names: list[str] = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[Any]:
        if command[:2] == ["docker", "compose"]:
            names.append(command[-2])
            command = [*command[:2], "--file", str(compose), *command[2:]]
        return real_run(command, **kwargs)

    monkeypatch.setenv("COMMERCE_STAGE_TIMEOUT_SECONDS", "3")
    monkeypatch.setattr(pipeline.subprocess, "run", run)
    try:
        for _ in range(2):
            with pytest.raises(pipeline.CommercePipelineError, match="container removed"):
                pipeline.run_compose_stage(
                    "bronze-input-sync",
                    {"batch_id": "a" * 16, "manifest_sha256": "b" * 64}, bucket="unused",
                )
            inspected = real_run(
                ["docker", "inspect", names[-1]], check=False,
                capture_output=True, text=True, timeout=10,
            )
            assert inspected.returncode != 0
            assert "no such object" in inspected.stderr.lower()
        assert len(set(names)) == 2
    finally:
        for name in names:
            real_run(
                ["docker", "rm", "--force", name], check=False, capture_output=True, timeout=10,
            )
