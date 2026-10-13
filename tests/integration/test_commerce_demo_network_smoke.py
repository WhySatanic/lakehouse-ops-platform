"""Real Docker network-release smoke; no stack reset or volume removal."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from lakehouse_ops.commerce_linked_demo import SERVICES, demo, release_demo_network


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


@pytest.mark.skipif(os.environ.get("LAKEOPS_RUN_DEMO_NETWORK_SMOKE") != "1",
                    reason="requires local Docker and the pinned Spark image")
@pytest.mark.parametrize("failure", [None, "start", "child"])
def test_demo_removes_containers_but_can_reopen_retained_data(tmp_path, failure):
    """Use the actual launcher lifecycle, with sleepers instead of Spark/Trino jobs."""
    image = "lakehouse-ops/spark-iceberg:3.5.9-1.11.0"
    source = tmp_path / "src/lakehouse_ops/commerce_linked_acceptance.py"
    source.parent.mkdir(parents=True)
    source.touch()
    services = {name: {"image": image, "entrypoint": ["sleep"], "command": ["120"],
                      "restart": "on-failure"} for name in SERVICES}
    services["minio"]["depends_on"] = {
        "minio-init": {"condition": "service_completed_successfully"},
    }
    services["minio-init"] = {
        "image": image, "user": "0", "entrypoint": ["/bin/sh", "-c"],
        "command": ["echo retained > /probe/marker"], "volumes": ["data:/probe"],
    }
    (tmp_path / "compose.yaml").write_text(json.dumps({
        "services": services, "volumes": {"data": {}},
    }))
    project = None

    def docker(*args, check=True):
        return subprocess.run(["docker", *args], check=check, capture_output=True,
                              text=True, timeout=45)

    def run(command, **kwargs):
        nonlocal project
        project = kwargs["env"]["COMPOSE_PROJECT_NAME"]
        if command[1] == "-m":
            acceptance = tmp_path / "evidence/acceptance"
            acceptance.mkdir()
            (acceptance / "linked.json").write_text(json.dumps({"status": "ready"}))
            return SimpleNamespace(returncode=int(failure == "child"))
        result = subprocess.run(command, **kwargs)
        if "up" in command and failure == "start":
            assert result.returncode == 0
            return SimpleNamespace(returncode=1)
        return result

    try:
        report = demo(tmp_path, tmp_path / "evidence", run=run)
        assert report["status"] == ("ready" if failure is None else "failed")
        assert report["network_cleanup"]["status"] == "absent"
        project_filter = f"label=com.docker.compose.project={project}"
        assert docker("ps", "-aq", "--filter", project_filter).stdout.strip() == ""
        assert docker("network", "inspect", f"{project}_default", check=False).returncode != 0
        volume = f"{project}_data"
        docker("volume", "inspect", volume)
        assert docker("run", "--rm", "--pull", "never", "--mount",
                      f"type=volume,src={volume},dst=/probe", "--entrypoint", "cat",
                      image, "/probe/marker").stdout.strip() == "retained"
        compose = ["compose", "--project-name", project, "-f", str(tmp_path / "compose.yaml")]
        docker(*compose, "up", "-d", "--wait", "--no-build", "--pull", "never", *SERVICES)
        assert len(docker("ps", "-q", "--filter", project_filter).stdout.splitlines()) == 5
        docker(*compose, "down")
        docker("volume", "inspect", volume)
        assert docker("ps", "-aq", "--filter", project_filter).stdout.strip() == ""
    finally:
        if project:
            docker("compose", "--project-name", project, "-f", str(tmp_path / "compose.yaml"),
                   "down", check=False)
