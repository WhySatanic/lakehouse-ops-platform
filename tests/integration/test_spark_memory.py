"""Opt-in local container acceptance with actual JVM and Python execution."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.skipif(
    os.getenv("LAKEOPS_RUN_SPARK_MEMORY_SMOKE") != "1", reason="requires local Spark Docker image",
)
def test_real_spark_session_emits_container_memory():
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run([
        "docker", "run", "--rm", "--memory", "2g", "--cpus", "2", "--network", "none",
        "--cgroupns", "private", "--hostname", "lakeops-memory",
        "--add-host", "lakeops-memory:127.0.0.1", "-e", "SPARK_LOCAL_IP=127.0.0.1",
        "--mount", f"type=bind,source={root},target=/repo,readonly",
        "-e", "PYTHONPATH=/repo/jobs/spark", "-e", "LAKEOPS_SPARK_MEMORY_REPORT=1",
        "-e", "LAKEHOUSE_BUCKET=unused", "-e", "LAKEOPS_S3_ENDPOINT_URL=http://localhost:9000",
        "-e", "AWS_ACCESS_KEY_ID=unused", "-e", "AWS_SECRET_ACCESS_KEY=unused",
        "-e", "HIVE_METASTORE_URI=thrift://localhost:9083",
        "lakehouse-ops/spark-iceberg:3.5.9-1.11.0", "/opt/spark/bin/spark-submit",
        "/repo/tests/integration/check_spark_memory.py",
    ], check=True, capture_output=True, text=True, timeout=180)
    reports = [json.loads(line) for line in (result.stdout + result.stderr).splitlines()
               if line.startswith('{"app_name":')]
    assert len(reports) == 1
    report = reports[0]
    assert report["kind"] == "spark_container_memory"
    assert report["status"] == "observed"
    assert report["app_name"] == "spark-container-memory-smoke"
    assert report["scope"] == "private_cgroup_v2_container_lifetime"
    assert report["peak_bytes"] > 0
    assert report["current_bytes"] >= 0
    assert report["elapsed_seconds"] > 0
    print(json.dumps(report, sort_keys=True))
