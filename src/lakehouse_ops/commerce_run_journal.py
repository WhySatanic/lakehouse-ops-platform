"""Local append-only execution evidence, separate from processing checkpoints."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4


class CommerceJournalError(OSError):
    pass


class CommerceRunJournal:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.run_id = uuid4().hex
        self.phase = "planning"
        self.batch: dict[str, str] = {}

    def record(
        self, phase: str, event: str, *, batch: dict[str, str] | None = None,
        error_type: str | None = None,
    ) -> None:
        if batch is not None:
            self.batch = dict(batch)
        self.phase = phase
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with closing(sqlite3.connect(self.path, timeout=5)) as connection, connection:
                connection.execute("PRAGMA synchronous=FULL")
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS commerce_run_events ("
                    "sequence INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, "
                    "recorded_at TEXT NOT NULL, batch_id TEXT, manifest_sha256 TEXT, "
                    "phase TEXT NOT NULL, event TEXT NOT NULL, error_type TEXT)"
                )
                connection.execute(
                    "INSERT INTO commerce_run_events "
                    "(run_id, recorded_at, batch_id, manifest_sha256, phase, event, error_type) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (self.run_id, datetime.now(UTC).isoformat(), self.batch.get("batch_id"),
                     self.batch.get("manifest_sha256"), phase, event, error_type),
                )
        except sqlite3.Error as error:
            raise CommerceJournalError(f"commerce journal write failed: {self.path}") from error
