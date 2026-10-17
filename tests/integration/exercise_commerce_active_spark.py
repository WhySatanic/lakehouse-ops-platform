"""Opt-in active SCD2 container interruption on a fresh, explicitly scoped stack."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import threading
import time
from concurrent.futures import Future
from datetime import UTC, datetime
from pathlib import Path

import boto3

from lakehouse_ops.commerce_change_scenario import generate_change_scenario
from lakehouse_ops.commerce_container_inspection import inspect_containers
from lakehouse_ops.commerce_linked_acceptance import verify_snapshot
from lakehouse_ops.commerce_pipeline import (
    CURRENT_STAGE,
    CommercePipelineError,
    run_commerce_batch,
    run_compose_stage,
)
from lakehouse_ops.commerce_recovery import audit_tables
from lakehouse_ops.commerce_run_journal import CommerceRunJournal
from lakehouse_ops.commerce_runner_lock import commerce_runner_lock
from lakehouse_ops.ingestion.commerce_batches import CommerceBatchPlanner
from lakehouse_ops.ingestion.commerce_s3_landing import CommerceS3LandingZone
from lakehouse_ops.trino import TrinoClient

STAGE = "spark-commerce-customer-scd2"
PROBE = """
import json, urllib.request
def read(path):
    with urllib.request.urlopen('http://127.0.0.1:4040/api/v1/' + path, timeout=1) as r:
        return json.load(r)
apps = read('applications')
jobs = [dict(job, application_id=app['id']) for app in apps
        for job in read('applications/' + app['id'] + '/jobs?status=running')]
print(json.dumps(jobs))
"""


class ActiveInterruption(RuntimeError):
    pass


def kill_if_active(journal, project, *, run=subprocess.run, probe_observation=None):
    """Probe only this attempt's recorded SCD2 container, then kill its immutable ID."""
    observation = {} if probe_observation is None else probe_observation
    observation["outcome"] = "launch_pending"
    with sqlite3.connect(journal.path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        launches = db.execute(
            "SELECT container_name FROM commerce_container_events "
            "WHERE run_id=? AND service=? AND event='launch_intent'",
            (journal.run_id, STAGE),
        ).fetchall()
    if not launches:
        return None
    if len(launches) != 1:
        raise ValueError("ambiguous SCD2 launch intent")
    name = launches[0][0]

    def owned():
        report = inspect_containers(journal.path, project=project, run_id=journal.run_id,
                                    container_name=name, limit=2, run=run)
        if report["has_more"] or len(report["containers"]) != 1:
            raise ValueError("requires one owned stage")
        return report["containers"][0]

    item = owned()
    observation.update(outcome="container_observed", state=item["observation"])
    if item["observation"] in {"absent", "created"}:
        return None
    if item["observation"] != "running" or item["service"] != STAGE:
        raise ValueError("stage ownership or running state unconfirmed")
    identity = item["container_id"]
    if re.fullmatch(r"[0-9a-f]{64}", identity) is None:
        raise ValueError("invalid immutable container ID")
    probe = run(["docker", "exec", identity, "python3", "-c", PROBE],
                capture_output=True, text=True, timeout=5, check=False)
    if probe.returncode:
        observation.update(outcome="probe_failed", returncode=probe.returncode,
                           stderr_tail=probe.stderr[-512:])
        return None  # UI may not yet exist; a deadline bounds retries.
    jobs = json.loads(probe.stdout)
    if not isinstance(jobs, list):
        raise ValueError("invalid Spark job response")
    observation.update(outcome="no_active_tasks", jobs_count=len(jobs), jobs=[
        {key: job.get(key) for key in ("status", "numActiveTasks", "application_id")}
        for job in jobs[:20]
    ])
    active = [job for job in jobs if job.get("status") == "RUNNING"
              and type(job.get("numActiveTasks")) is int and job["numActiveTasks"] > 0
              and isinstance(job.get("application_id"), str) and job["application_id"]]
    if not active:
        return None
    observation["outcome"] = "active_tasks_observed"
    confirmed = owned()
    if confirmed.get("container_id") != identity or confirmed["observation"] != "running":
        raise ValueError("stage changed after active Spark observation")
    evidence = {**item, "active_jobs": active, "observed_at": datetime.now(UTC).isoformat()}
    killed = run(["docker", "container", "kill", "--signal", "KILL", "--", identity],
                 capture_output=True, text=True, timeout=10, check=False)
    if killed.returncode:
        raise ValueError("active container termination failed")
    observation["outcome"] = "kill_accepted"
    return {**evidence, "kill_accepted_at": datetime.now(UTC).isoformat()}


def interrupt_stage(stage, journal, project, evidence, *, timeout=180):
    done = threading.Event()
    result = Future()
    started = time.monotonic()
    observation = {"attempts": 0, "outcomes": {}}
    evidence["observer_diagnostics"] = observation

    def observe():
        try:
            deadline = time.monotonic() + timeout
            while not done.is_set() and time.monotonic() < deadline:
                probe_observation = {"outcome": "probe_error"}
                probe_started = time.monotonic()
                try:
                    sample = kill_if_active(journal, project,
                                            probe_observation=probe_observation)
                finally:
                    probe_observation["elapsed_seconds"] = time.monotonic() - probe_started
                    observation["attempts"] += 1
                    outcome = probe_observation["outcome"]
                    counts = observation["outcomes"]
                    counts[outcome] = counts.get(outcome, 0) + 1
                    observation["last_probe"] = probe_observation
                if sample:
                    result.set_result(sample)
                    return
                done.wait(0.1)
            raise TimeoutError("active Spark boundary was not observed; no verified retry")
        except BaseException as error:
            result.set_exception(error)

    watcher = threading.Thread(target=observe, daemon=True, name="active-spark-observer")
    watcher.start()
    failure = None
    try:
        stage()
    except CommercePipelineError as error:
        failure = error
    finally:
        done.set()
        watcher.join(25)
        observation.update(elapsed_seconds=time.monotonic() - started,
                           stage_failed=failure is not None)
    if watcher.is_alive():
        raise RuntimeError("observer still active; no retry allowed")
    evidence.update(result.result())
    if failure is None:
        raise ValueError("stage did not fail after observed interruption")
    evidence["stage_error_type"] = type(failure).__name__
    raise ActiveInterruption("observed active Spark container was terminated") from failure


def exercise(output, endpoint, server, report):
    project = os.environ.get("COMPOSE_PROJECT_NAME", "")
    if not project.startswith("lakehouse-ops-active-spark-"):
        raise ValueError("requires dedicated lakehouse-ops-active-spark- project")
    with commerce_runner_lock(), TrinoClient(server, query_timeout=120) as trino:
        if audit_tables(trino.query):
            raise ValueError("requires fresh commerce catalog")
        generate_change_scenario(output / "scenario")
        expected = json.loads((output / "scenario/expected.json").read_text())
        s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")
        landing = CommerceS3LandingZone(s3, bucket="lakehouse")
        for batch in expected["batches"]:
            landing.write(output / "scenario" / f"batch_id={batch['batch_id']}")
        state = output / "checkpoint.json"
        journal_path = output / "journal.sqlite3"

        def planner():
            return CommerceBatchPlanner(s3, bucket="lakehouse", state_path=state)

        report.update(project=project, samples=[], scope="observed_active_spark_container_kill",
                      source_sha=subprocess.check_output(["git", "rev-parse", "HEAD"],
                                                         text=True).strip())
        for index, batch in enumerate(expected["batches"]):
            if index == 1:
                before = state.read_bytes()
                report["before_failure"] = audit_tables(trino.query)
                report["checkpoint_before_sha256"] = hashlib.sha256(before).hexdigest()
                journal = CommerceRunJournal(journal_path)
                report["interruption"] = {"run_id": journal.run_id}

                def interrupted(service, selected, *, attempt=journal):
                    def stage():
                        run_compose_stage(service, selected, bucket="lakehouse", journal=attempt)

                    if service == STAGE:
                        interrupt_stage(stage, attempt, project, report["interruption"])
                    else:
                        stage()

                try:
                    run_commerce_batch(
                        planner(), trino.query, run_stage=interrupted, journal=journal,
                    )
                except ActiveInterruption:
                    pass
                else:
                    raise ValueError("active interruption not reached")
                report["checkpoint_after_failure_sha256"] = hashlib.sha256(
                    state.read_bytes(),
                ).hexdigest()
                if state.read_bytes() != before:
                    raise ValueError("active failure advanced checkpoint")
                report["after_failure"] = audit_tables(trino.query)
            journal = CommerceRunJournal(journal_path)
            sample = {"run_id": journal.run_id, "batch_id": batch["batch_id"]}
            report["samples"].append(sample)

            def stage(service, selected, *, wanted=batch, attempt=journal,
                      ordinal=index, current_sample=sample):
                if selected["batch_id"] != wanted["batch_id"]:
                    raise ValueError("retry selected a different batch")
                run_compose_stage(service, selected, bucket="lakehouse", journal=attempt)
                if service == CURRENT_STAGE:
                    current_sample["oracle"] = verify_snapshot(
                        trino.query, expected, ordinal, wanted["batch_id"],
                    )

            sample["pipeline"] = run_commerce_batch(
                planner(), trino.query, run_stage=stage, journal=journal,
            )
            if "oracle" not in sample or sample["pipeline"]["status"] != "ready":
                raise ValueError("literal oracle and verified checkpoint required")
        report["after_retry"] = audit_tables(trino.query)

        def forbidden(*_):
            raise ValueError("idle replay started compute")

        report["idle"] = run_commerce_batch(planner(), trino.query, run_stage=forbidden)
        if report["idle"]["status"] != "idle":
            raise ValueError("final incremental replay was not idle")
        report["status"] = "ready"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--s3-endpoint-url", required=True)
    parser.add_argument("--server", required=True)
    args = parser.parse_args()
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("requires empty evidence output")
    args.output.mkdir(parents=True, exist_ok=True)
    report = {"status": "failed", "started_at": datetime.now(UTC).isoformat(),
              "limitations": ["not injection inside Iceberg commit", "not host/power failure",
                              "not distributed exactly-once", "requires active Spark UI tasks"]}
    try:
        exercise(args.output, args.s3_endpoint_url, args.server, report)
    except Exception as error:
        report.update(error_type=type(error).__name__, error=str(error))
    finally:
        (args.output / "active-spark.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8",
        )
    return 0 if report["status"] == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())
