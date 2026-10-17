"""Short POSIX descendant-lifetime regression; no Spark tasks or Iceberg writes."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from lakehouse_ops.commerce_history_process import run_profile_process


def exercise(output):
    if os.name != "posix":
        raise ValueError("requires POSIX process groups")
    output.mkdir(parents=True, exist_ok=False)
    marker, ready = output / "continued.txt", output / "child.txt"
    child = (
        "import os,time; from pathlib import Path; "
        f"Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(0.9); "
        f"Path({str(marker)!r}).write_text('continued')"
    )
    parent = (
        "import subprocess,sys,time; from pathlib import Path; "
        f"subprocess.Popen([sys.executable,'-c',{child!r}]); time.sleep(60)"
    )
    report = {"status": "failed", "scope": "short POSIX controller tree; no Docker stage or commit"}
    started = time.monotonic()
    with (output / "process.log").open("wb") as log:
        try:
            run_profile_process([sys.executable, "-c", parent], stdout=log,
                                stderr=subprocess.STDOUT, timeout=0.5, check=True)
        except subprocess.TimeoutExpired as error:
            report.update(timeout_preserved=True,
                          profile_process_group=getattr(error, "profile_process_group", None))
        else:
            raise AssertionError("expected profile timeout")
    report["child_observed"] = ready.is_file()
    time.sleep(1.2)
    report["descendant_continued"] = marker.exists()
    report["elapsed_seconds"] = time.monotonic() - started
    if report["child_observed"] and not report["descendant_continued"]:
        report["status"] = "verified"
    (output / "process-tree.json").write_text(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = exercise(args.output.resolve())
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result["status"] == "verified" else 2)
