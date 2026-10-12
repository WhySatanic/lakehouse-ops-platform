"""Opt-in real MinIO/Compose selected-batch mirroring and default full-landing replay."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from uuid import uuid4

import boto3
import pytest

from lakehouse_ops.commerce_change_scenario import generate_change_scenario
from lakehouse_ops.commerce_linked_demo import free_ports
from lakehouse_ops.ingestion.commerce_s3_landing import CommerceS3LandingZone

READER = """
import hashlib, json
from pathlib import Path
print(json.dumps({p.relative_to('/input').as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in Path('/input').rglob('*') if p.is_file()}, sort_keys=True))
"""


@pytest.mark.skipif(os.environ.get("LAKEOPS_RUN_SELECTED_SYNC_SMOKE") != "1",
                    reason="requires local Docker and MinIO tools image")
def test_selected_batch_and_legacy_full_sync_keep_exact_bytes(tmp_path, monkeypatch):
    root = Path.cwd()
    project = "lakehouse-ops-selected-sync-" + uuid4().hex
    env = {**os.environ, **free_ports(), "COMPOSE_PROJECT_NAME": project,
           "COMPOSE_FILE": str(root / "compose.yaml"), "AWS_ACCESS_KEY_ID": "lakeops-ingest",
           "AWS_SECRET_ACCESS_KEY": "ingest-development-only"}
    env.pop("AWS_SESSION_TOKEN", None)
    env.pop("COMPOSE_PROFILES", None)
    compose = ["docker", "compose", "--profile", "catalog", "--profile", "compute"]

    def run(command, *, selected_env=env, check=True):
        return subprocess.run(command, env=selected_env, check=check, capture_output=True,
                              text=True, encoding="utf-8", timeout=180)

    def contents(volume):
        return json.loads(run([
            "docker", "run", "--rm", "--network", "none", "--entrypoint", "python3",
            "--mount", f"type=volume,source={volume},target=/input,readonly",
            "lakehouse-ops/spark-iceberg:3.5.9-1.11.0", "-c", READER,
        ]).stdout)

    try:
        run([*compose, "up", "-d", "--wait", "--no-build", "--pull", "never", "minio"])
        run([*compose, "run", "--rm", "--no-deps", "minio-init"])
        scenario = generate_change_scenario(tmp_path / "scenario")
        s3 = boto3.client("s3", endpoint_url=f"http://localhost:{env['MINIO_API_PORT']}",
                          region_name="us-east-1", aws_access_key_id="lakeops-ingest",
                          aws_secret_access_key="ingest-development-only")
        landing = CommerceS3LandingZone(s3, bucket="lakehouse")
        expected = {}
        for batch in scenario["batches"]:
            fixture = Path(batch["path"])
            landing.write(fixture)
            expected.update({f"source=commerce/batch_id={batch['batch_id']}/{p.name}":
                             hashlib.sha256(p.read_bytes()).hexdigest() for p in fixture.iterdir()})
        selected = scenario["batches"][1]["batch_id"]
        selected_files = {key: value for key, value in expected.items()
                          if f"batch_id={selected}/" in key}
        evidence = []
        for scope in ("selected", "all"):
            volume = project + "-" + scope
            child = {**env, "COMMERCE_BATCH_ID": selected, "COMMERCE_SYNC_SCOPE": scope}
            for _ in range(2):
                result = run([*compose, "run", "--rm", "--no-deps", "--volume", f"{volume}:/input",
                              "bronze-input-sync"], selected_env=child)
                report = json.loads(result.stdout.splitlines()[-1])
                assert report["scope"] == ("selected_commerce_batch" if scope == "selected"
                                           else "all_landing")
                assert report["landing_objects"] == (1 if scope == "selected" else 3)
                assert contents(volume) == (selected_files if scope == "selected" else expected)
            evidence.append({"scope": scope, "files": len(contents(volume))})
        invalid = {**env, "COMMERCE_BATCH_ID": "../bad", "COMMERCE_SYNC_SCOPE": "selected"}
        assert run([*compose, "run", "--rm", "--no-deps", "bronze-input-sync"],
                   selected_env=invalid, check=False).returncode != 0
        proof = os.environ.get("LAKEOPS_SELECTED_SYNC_EVIDENCE")
        if proof:
            Path(proof).write_text(json.dumps({"status": "ready", "project": project,
                                              "samples": evidence}, indent=2), encoding="utf-8")
    finally:
        run([*compose, "stop", "minio"], check=False)
