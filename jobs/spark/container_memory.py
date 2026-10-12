"""Opt-in diagnostics for a private cgroup-v2 Spark container, not Python heap."""

from __future__ import annotations

import atexit
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


def memory_observation(
    root: Path = Path("/sys/fs/cgroup"),
    membership: Path = Path("/proc/self/cgroup"),
) -> dict[str, Any]:
    unavailable = {"status": "unavailable", "peak_bytes": None, "current_bytes": None}
    try:
        # Only support the namespace-root layout used by private Docker cgroups.
        if membership.read_text().strip() != "0::/":
            return unavailable
        peak = int((root / "memory.peak").read_text().strip())
        current = int((root / "memory.current").read_text().strip())
        if peak < 0 or current < 0:
            return unavailable
    except (OSError, ValueError):
        return unavailable
    return {"status": "observed", "peak_bytes": peak, "current_bytes": current}


def register_memory_report(app_name: str) -> None:
    if os.environ.get("LAKEOPS_SPARK_MEMORY_REPORT") != "1":
        return
    started = time.perf_counter()

    def emit() -> None:
        report = {
            "kind": "spark_container_memory", "schema_version": "1.0",
            "app_name": app_name, "batch_id": os.environ.get("COMMERCE_BATCH_ID"),
            "scope": "private_cgroup_v2_container_lifetime",
            "elapsed_seconds": time.perf_counter() - started,
            **memory_observation(),
        }
        print(json.dumps(report, sort_keys=True), file=sys.stderr, flush=True)

    atexit.register(emit)
