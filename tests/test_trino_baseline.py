from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakehouse_ops.trino import TrinoQueryResult, TrinoQueryStats
from lakehouse_ops.trino_baseline import (
    QueryCorpusError,
    capture_baseline,
    load_query_corpus,
)


class FakeTrinoClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def query_with_stats(self, sql: str) -> TrinoQueryResult:
        self.queries.append(sql)
        index = len(self.queries)
        return TrinoQueryResult(
            query_id=f"query-{index}",
            rows=({"Query Plan": f"Fragment {index}\nScanFilterProject"},),
            stats=TrinoQueryStats(
                state="FINISHED",
                elapsed_time_ms=20 + index,
                wall_time_ms=10 + index,
                cpu_time_ms=5 + index,
                processed_rows=3,
                processed_bytes=128,
                physical_input_bytes=64,
                peak_memory_bytes=256,
                spilled_bytes=0,
            ),
        )


def write_corpus(path: Path, queries: list[dict[str, str]]) -> None:
    path.write_text(
        json.dumps(
            {"schema_version": "1.0", "name": "test_corpus", "queries": queries}
        ),
        encoding="utf-8",
    )


def test_load_and_capture_query_baseline(tmp_path: Path) -> None:
    path = tmp_path / "corpus.json"
    write_corpus(
        path,
        [
            {"id": "scan_query", "description": "Scan rows", "sql": "SELECT 1"},
            {
                "id": "group_query",
                "description": "Group rows",
                "sql": "WITH values AS (SELECT 1 AS id) SELECT count(*) FROM values",
            },
        ],
    )
    corpus = load_query_corpus(path)
    client = FakeTrinoClient()

    report = capture_baseline(
        client,
        corpus,
        clock=lambda: datetime(2026, 8, 25, 3, 0, tzinfo=UTC),
    )

    assert client.queries == [
        "EXPLAIN ANALYZE SELECT 1",
        "EXPLAIN ANALYZE WITH values AS (SELECT 1 AS id) SELECT count(*) FROM values",
    ]
    assert report["corpus"] == {
        "schema_version": "1.0",
        "name": "test_corpus",
        "query_count": 2,
    }
    assert report["queries"][0]["metrics"]["wall_time_ms"] == 11
    assert report["queries"][0]["plan_line_count"] == 2
    assert len(report["queries"][0]["sql_sha256"]) == 64
    assert report["collected_at"] == "2026-08-25T03:00:00+00:00"


@pytest.mark.parametrize(
    "queries, message",
    [
        ([], "between 1 and 25"),
        (
            [
                {"id": "same_id", "description": "One", "sql": "SELECT 1"},
                {"id": "same_id", "description": "Two", "sql": "SELECT 2"},
            ],
            "duplicate query ID",
        ),
        (
            [{"id": "delete_query", "description": "Unsafe", "sql": "DELETE FROM x"}],
            "single read-only",
        ),
        (
            [{"id": "multi_query", "description": "Unsafe", "sql": "SELECT 1; SELECT 2"}],
            "single read-only",
        ),
    ],
)
def test_query_corpus_rejects_invalid_contract(
    tmp_path: Path, queries: list[dict[str, str]], message: str
) -> None:
    path = tmp_path / "corpus.json"
    write_corpus(path, queries)

    with pytest.raises(QueryCorpusError, match=message):
        load_query_corpus(path)


def test_query_corpus_rejects_invalid_document_metadata(tmp_path: Path) -> None:
    path = tmp_path / "corpus.json"
    path.write_text("not json", encoding="utf-8")
    with pytest.raises(QueryCorpusError):
        load_query_corpus(path)

    path.write_text(json.dumps({"schema_version": "2.0"}), encoding="utf-8")
    with pytest.raises(QueryCorpusError, match="schema_version"):
        load_query_corpus(path)

    path.write_text(
        json.dumps({"schema_version": "1.0", "name": "", "queries": []}),
        encoding="utf-8",
    )
    with pytest.raises(QueryCorpusError, match="name must be non-empty"):
        load_query_corpus(path)
