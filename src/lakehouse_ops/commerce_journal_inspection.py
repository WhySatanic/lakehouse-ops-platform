"""Inspect recent commerce execution attempts without modifying their SQLite journal."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from typing import Any

from lakehouse_ops.commerce_run_journal import CommerceJournalError
from lakehouse_ops.ingestion.commerce_batches import _load_state


def inspect_journal(
    path: Path, *, limit: int = 10, checkpoint: Path | None = None,
) -> dict[str, Any]:
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("limit must be an integer between 1 and 100")
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True,
                                     timeout=5)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            rows = connection.execute(
                "WITH attempts AS ("
                "SELECT run_id, min(sequence) AS first_sequence, max(sequence) AS last_sequence, "
                "count(*) AS event_count, "
                "sum(CASE WHEN phase='checkpoint' AND event='completed' THEN 1 ELSE 0 END) "
                "AS checkpoint_completed_events FROM commerce_run_events GROUP BY run_id "
                "ORDER BY last_sequence DESC LIMIT ?) "
                "SELECT a.*, first.recorded_at AS started_at, "
                "last.recorded_at AS last_recorded_at, "
                "last.phase, last.event, last.error_type, last.batch_id, last.manifest_sha256 "
                "FROM attempts a JOIN commerce_run_events first ON first.sequence=a.first_sequence "
                "JOIN commerce_run_events last ON last.sequence=a.last_sequence "
                "ORDER BY a.last_sequence DESC", (limit,),
            ).fetchall()
    except sqlite3.Error as error:
        raise CommerceJournalError(f"cannot inspect commerce journal: {path}: {error}") from error
    runs = []
    for row in rows:
        run = dict(row)
        run["outcome"] = (
            row["event"] if row["phase"] == "run" and row["event"] in {"ready", "idle"}
            else "failed" if row["event"] == "failed" else "unfinished"
        )
        runs.append(run)
    report = {
        "status": "observed", "scope": "diagnostic_journal_not_checkpoint_or_liveness",
        "journal": str(path.resolve()), "limit": limit, "runs": runs,
    }
    if checkpoint is not None:
        processed = _load_state(checkpoint)["processed_batches"]
        for run in runs:
            entry = processed.get(run["batch_id"])
            run["checkpoint_observation"] = (
                "not_batch_bound" if run["batch_id"] is None or run["manifest_sha256"] is None
                else "not_recorded" if entry is None
                else "matching_manifest" if entry["manifest_sha256"] == run["manifest_sha256"]
                else "different_manifest"
            )
        report.update(scope="diagnostic_journal_and_checkpoint_not_atomic_or_liveness",
                      checkpoint=str(checkpoint.resolve()))
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--checkpoint", type=Path)
    args = parser.parse_args(argv)
    try:
        report = inspect_journal(args.journal, limit=args.limit, checkpoint=args.checkpoint)
    except (OSError, ValueError) as error:
        print(f"commerce journal inspection failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
