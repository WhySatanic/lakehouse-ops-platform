"""Real Docker network-release smoke; no stack reset or volume removal."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

from lakehouse_ops.commerce_linked_demo import release_demo_network


@pytest.mark.skipif(os.environ.get("LAKEOPS_RUN_DEMO_NETWORK_SMOKE") != "1",
                    reason="requires local Docker and the pinned Spark image")
def test_network_release_refuses_active_endpoint_and_preserves_volume():
    project = f"lakehouse-ops-linked-demo-{uuid4().hex}"
    network, volume, container = f"{project}_default", f"{project}_data", f"{project}-probe"
    image = "lakehouse-ops/spark-iceberg:3.5.9-1.11.0"

    def docker(*args, check=True):
        return subprocess.run(["docker", *args], check=check, capture_output=True,
                              text=True, timeout=30)

    docker("image", "inspect", image)
    docker("network", "create", "--label", f"com.docker.compose.project={project}", network)
    docker("volume", "create", "--label", f"com.docker.compose.project={project}", volume)
    try:
        docker("run", "--detach", "--rm", "--pull", "never", "--name", container,
               "--network", network, "--entrypoint", "sleep", image, "120")
        with pytest.raises(RuntimeError, match="attached containers"):
            release_demo_network(project, Path.cwd(), dict(os.environ), subprocess.run)
        docker("network", "inspect", network)
        docker("inspect", container)
        docker("rm", "--force", container)
        assert release_demo_network(project, Path.cwd(), dict(os.environ), subprocess.run) == {
            "network": network, "status": "released",
        }
        assert docker("network", "inspect", network, check=False).returncode != 0
        docker("volume", "inspect", volume)
        assert release_demo_network(project, Path.cwd(), dict(os.environ), subprocess.run) == {
            "network": network, "status": "absent",
        }
        print(f"retained_volume={volume}")
    finally:
        docker("rm", "--force", container, check=False)
        release_demo_network(project, Path.cwd(), dict(os.environ), subprocess.run)
        # The uniquely named empty volume remains as proof that teardown did not delete it.
