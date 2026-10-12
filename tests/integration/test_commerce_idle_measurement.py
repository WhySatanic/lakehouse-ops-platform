"""Time three idle children on a retained, already completed local scale stack."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from lakehouse_ops.commerce_pipeline_profile import measure_idle_replay
from lakehouse_ops.trino import TrinoClient


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_COMMERCE_IDLE_PROFILE_SMOKE") != "1",
    reason="requires local scale stack",
)
def test_real_idle_children_preserve_checkpoint_files_and_snapshots(tmp_path: Path) -> None:
    source = Path(os.environ["LAKEOPS_IDLE_PROFILE_CHECKPOINT"])
    output = Path(os.environ.get("LAKEOPS_IDLE_PROFILE_OUTPUT", str(tmp_path)))
    output.mkdir(parents=True, exist_ok=True)
    checkpoint = output / "checkpoint.json"
    original = source.read_bytes()
    checkpoint.write_bytes(original)
    server = os.environ["LAKEOPS_IDLE_PROFILE_SERVER"]
    command = [
        "run-commerce-batch", "--state", str(checkpoint), "--s3-bucket", "lakehouse",
        "--s3-endpoint-url", os.environ["LAKEOPS_IDLE_PROFILE_ENDPOINT"],
        "--server", server, "--user", "lakehouse-performance",
    ]
    with TrinoClient(server, user="lakehouse-performance") as client:
        observations = [
            measure_idle_replay(client, command, output, f"idle-{index + 1}")
            for index in range(3)
        ]
    assert checkpoint.read_bytes() == source.read_bytes() == original
    assert all(row["status"] == "verified" and row["elapsed_seconds"] > 0 for row in observations)
    assert observations[0]["snapshots"] == observations[-1]["snapshots"]
    # The scale fixture has 13 tables; the linked scenario adds current gold separately.
    assert len(observations[0]["snapshots"]) == 13
    (output / "idle-profile.json").write_text(json.dumps({
        "scope": "three_sequential_idle_children_on_retained_stack_not_capacity",
        "observations": observations, "checkpoint_unchanged": True,
    }, indent=2), encoding="utf-8")
