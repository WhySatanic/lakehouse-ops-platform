"""Observe a real owned Docker sleeper after an injected profile-child timeout.

This is not Spark task execution, Iceberg recovery or automatic stage containment.
Run from the repository root with the existing cached Spark image.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import runpy
import subprocess
import sys
from pathlib import Path

from lakehouse_ops.commerce_run_journal import CommerceRunJournal

IMAGE = "lakehouse-ops/spark-iceberg:3.5.9-1.11.0"


def exercise(output):
    if output.exists():
        raise ValueError("requires a new evidence output directory")
    root = Path.cwd()
    state = {}
    calls = []
    report = {
        "status": "failed",
        "scope": "real Docker sleeper, injected Python child timeout; no Spark or Iceberg",
    }

    def actual(command, **options):
        return subprocess.run(
            command, check=True, capture_output=True, text=True, timeout=30, **options
        )

    def inventory():
        return actual(
            ["docker", "ps", "-a", "--no-trunc", "--format", "{{.ID}} {{.State}}"]
        ).stdout.splitlines()

    before = inventory()
    report["image_id"] = json.loads(actual(["docker", "image", "inspect", IMAGE]).stdout)[0]["Id"]
    module = runpy.run_path(str(root / "tests/integration/test_customer_history_experiment.py"))

    def run(command, **options):
        state["output_owned"] = True
        calls.append(command)
        env = options["env"]
        project = env["COMPOSE_PROJECT_NAME"]
        if command[1] == "-m":
            profile = output / "profile"
            profile.mkdir()
            (profile / "checkpoint.json").write_text('{"processed_batches": {}}')
            (profile / "checkpoint.json.active.json").write_text('{"active": true}')
            journal = CommerceRunJournal(profile / "checkpoint.json.runs.sqlite3")
            batch = {"batch_id": "a" * 16, "manifest_sha256": "b" * 64}
            name = "lakeops-commerce-" + project.rsplit("-", 1)[-1]
            service = "spark-commerce-order-silver"
            journal.record_container(
                service, name, "launch_intent", batch=batch, project_hint=project
            )
            state.update(project=project, journal=journal, journal_bytes=journal.path.read_bytes())
            state["network"] = actual(
                [
                    "docker",
                    "network",
                    "create",
                    "--label",
                    "com.docker.compose.project=" + project,
                    project + "_default",
                ]
            ).stdout.strip()
            labels = {
                "com.docker.compose.project": project,
                "com.docker.compose.service": service,
                "lakehouse.ops.run_id": journal.run_id,
                "lakehouse.ops.batch_id": batch["batch_id"],
                "lakehouse.ops.manifest_sha256": batch["manifest_sha256"],
            }
            state["labels"] = labels
            launch = [
                "docker",
                "run",
                "--pull",
                "never",
                "-d",
                "--name",
                name,
                "--network",
                state["network"],
            ]
            for key, value in labels.items():
                launch.extend(["--label", key + "=" + value])
            launch.extend(["--entrypoint", "python3", IMAGE, "-c", "import time; time.sleep(120)"])
            state["container"] = actual(launch).stdout.strip()
            subprocess.run(
                [
                    getattr(sys, "_base_executable", sys.executable),
                    "-c",
                    "import time; time.sleep(60)",
                ],
                timeout=0.2,
                check=True,
                capture_output=True,
            )
            raise AssertionError("expected child timeout")
        if command[1:3] in (["container", "inspect"], ["network", "inspect"], ["network", "rm"]):
            return subprocess.run(command, **options)
        # Base services and the long profile are deliberately not started in this surrogate.
        return subprocess.CompletedProcess(command, 0)

    try:
        try:
            module["experiment"](root, output, run=run)
        except subprocess.TimeoutExpired:
            pass
        else:
            raise AssertionError("profile child did not time out")
        evidence = json.loads((output / "experiment.json").read_bytes())
        observation = evidence["failure_containers"]
        item = observation["containers"][0]
        assert item["container_id"] == state["container"] and item["observation"] == "running"
        assert state["journal"].path.read_bytes() == state["journal_bytes"]
        assert (output / "profile/checkpoint.json").read_text() == '{"processed_batches": {}}'
        assert (output / "profile/checkpoint.json.active.json").read_text() == '{"active": true}'
        assert evidence["status"] == "failed" and evidence["error_type"] == "TimeoutExpired"
        assert "network has attached containers" in evidence["cleanup_error"]
        assert observation == json.loads((output / "containers-at-failure.json").read_bytes())
        assert not any("kill" in c or c[:2] == ["docker", "rm"] for c in calls)
        report.update(
            status="verified",
            observation=observation,
            timeout_preserved=True,
            original_wrapper_cleanup="incomplete; expected attached sleeper",
            checkpoint_and_intent_unchanged=True,
        )
    finally:
        if state.get("container"):
            data = json.loads(
                actual(["docker", "container", "inspect", "--", state["container"]]).stdout
            )[0]
            assert data["Id"] == state["container"]
            assert all(data["Config"]["Labels"].get(k) == v for k, v in state["labels"].items())
            actual(["docker", "rm", "--force", "--", state["container"]])
        if state.get("network"):
            data = json.loads(actual(["docker", "network", "inspect", state["network"]]).stdout)[0]
            assert data["Id"] == state["network"] and not data["Containers"]
            assert data["Labels"]["com.docker.compose.project"] == state["project"]
            actual(["docker", "network", "rm", state["network"]])
        report["foreign_states_preserved"] = inventory() == before
        report["foreign_container_count"] = len(before)
        report["operator_cleanup"] = "only rechecked owned immutable container/network IDs removed"
        if state.get("output_owned"):
            report["source_sha256"] = hashlib.sha256(
                (root / "tests/integration/test_customer_history_experiment.py").read_bytes()
            ).hexdigest()
            (output / "timeout-smoke.json").write_text(json.dumps(report, indent=2))
    assert report["status"] == "verified" and report["foreign_states_preserved"]
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(exercise(args.output.resolve()), indent=2))
