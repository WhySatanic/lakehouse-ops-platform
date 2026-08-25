from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any

import httpx

from lakehouse_ops.trino import TrinoClient
from lakehouse_ops.trino_cache_experiment import (
    capture_metadata_cache_experiment,
    validate_cache_catalog_pair,
)


def wait_for_cluster(server: str, cycle: int) -> dict[str, Any]:
    subprocess.run(
        ["docker", "compose", "restart", "trino-coordinator"],
        check=True,
        timeout=60,
    )
    deadline = time.monotonic() + 120
    coordinator_id: str | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{server.rstrip('/')}/v1/info", timeout=3)
            response.raise_for_status()
            info = response.json()
            if info.get("starting") is not False:
                time.sleep(1)
                continue
            coordinator_id = info.get("coordinatorId")
            with TrinoClient(server, user="metadata-cache-readiness", timeout=5) as client:
                rows = client.query(
                    "SELECT count(*) AS active_nodes FROM system.runtime.nodes "
                    "WHERE state = 'active'"
                )
            if len(rows) == 1 and rows[0].get("active_nodes") == 3:
                return {
                    "cycle": cycle,
                    "active_nodes": 3,
                    "coordinator_id": coordinator_id,
                }
        except (httpx.HTTPError, ValueError, RuntimeError):
            pass
        time.sleep(2)
    raise RuntimeError("Trino cluster did not recover after coordinator restart")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("server")
    parser.add_argument("report", type=Path)
    parser.add_argument("enabled_config", type=Path)
    parser.add_argument("disabled_config", type=Path)
    args = parser.parse_args()

    configuration = validate_cache_catalog_pair(
        args.enabled_config, args.disabled_config
    )
    report = capture_metadata_cache_experiment(
        lambda: TrinoClient(args.server, user="lakehouse-performance", timeout=60),
        lambda cycle: wait_for_cluster(args.server, cycle),
        enabled_catalog="lakehouse",
        disabled_catalog="lakehouse_cache_disabled",
        schema="ops",
        table="pruning_partitioned",
        configuration=configuration,
        cycles=3,
    )
    args.report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
