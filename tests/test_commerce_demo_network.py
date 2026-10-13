from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from lakehouse_ops.commerce_linked_demo import release_demo_network


@pytest.mark.parametrize("case", ["empty", "absent", "active", "foreign", "name",
                                 "malformed", "inspect_failed", "remove_failed"])
def test_demo_releases_only_its_verified_empty_network(tmp_path, case):
    project = "lakehouse-ops-linked-demo-test"
    name = f"{project}_default"
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        assert kwargs["timeout"] == 30 and kwargs["cwd"] == tmp_path
        assert kwargs["capture_output"] and kwargs["text"] and not kwargs["check"]
        if command[2] == "rm":
            return SimpleNamespace(returncode=int(case == "remove_failed"), stderr="rm failed")
        network = {"Name": "other" if case == "name" else name,
                   "Labels": {"com.docker.compose.project": (
                       "foreign" if case == "foreign" else project
                   )}, "Containers": {"writer": {}} if case == "active" else {}}
        return SimpleNamespace(
            returncode=int(case in ("absent", "inspect_failed")),
            stderr=f"network {name} not found" if case == "absent" else "daemon unavailable",
            stdout="invalid" if case == "malformed" else json.dumps([network]),
        )

    if case in ("empty", "absent"):
        result = release_demo_network(project, tmp_path, {}, run)
        assert result == {"network": name, "status": "absent" if case == "absent" else "released"}
    else:
        with pytest.raises((ValueError, RuntimeError)):
            release_demo_network(project, tmp_path, {}, run)
    assert commands == [["docker", "network", "inspect", name]] + (
        [["docker", "network", "rm", name]] if case in ("empty", "remove_failed") else []
    )
