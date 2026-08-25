from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lakehouse_ops.trino import TrinoClient

QUERY_ID = re.compile(r"^[a-z][a-z0-9_]{2,63}$")


class QueryCorpusError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class QueryDefinition:
    query_id: str
    description: str
    sql: str


@dataclass(frozen=True, slots=True)
class QueryCorpus:
    schema_version: str
    name: str
    queries: tuple[QueryDefinition, ...]


def load_query_corpus(path: Path) -> QueryCorpus:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise QueryCorpusError(str(error)) from error
    if not isinstance(payload, dict) or payload.get("schema_version") != "1.0":
        raise QueryCorpusError("query corpus schema_version must be 1.0")
    name = payload.get("name")
    if not isinstance(name, str) or not name.strip():
        raise QueryCorpusError("query corpus name must be non-empty")
    raw_queries = payload.get("queries")
    if not isinstance(raw_queries, list) or not 1 <= len(raw_queries) <= 25:
        raise QueryCorpusError("query corpus must contain between 1 and 25 queries")
    queries: list[QueryDefinition] = []
    seen: set[str] = set()
    for item in raw_queries:
        query = _query_definition(item)
        if query.query_id in seen:
            raise QueryCorpusError(f"duplicate query ID: {query.query_id}")
        seen.add(query.query_id)
        queries.append(query)
    return QueryCorpus("1.0", name.strip(), tuple(queries))


def capture_baseline(
    client: TrinoClient,
    corpus: QueryCorpus,
    *,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    now = clock or (lambda: datetime.now(UTC))
    runs: list[dict[str, Any]] = []
    for query in corpus.queries:
        result = client.query_with_stats(f"EXPLAIN ANALYZE {query.sql}")
        plan = "\n".join(str(next(iter(row.values()))) for row in result.rows)
        if not plan.strip():
            raise QueryCorpusError(f"query produced no EXPLAIN ANALYZE plan: {query.query_id}")
        stats = asdict(result.stats)
        runs.append(
            {
                "query_id": query.query_id,
                "description": query.description,
                "trino_query_id": result.query_id,
                "sql_sha256": _digest(query.sql),
                "plan_sha256": _digest(plan),
                "plan_line_count": len(plan.splitlines()),
                "metrics": stats,
            }
        )
    return {
        "schema_version": "1.0",
        "status": "ready",
        "collected_at": now().astimezone(UTC).isoformat(),
        "engine": "trino",
        "mode": "explain_analyze",
        "corpus": {
            "schema_version": corpus.schema_version,
            "name": corpus.name,
            "query_count": len(corpus.queries),
        },
        "queries": runs,
    }


def _query_definition(value: Any) -> QueryDefinition:
    if not isinstance(value, dict):
        raise QueryCorpusError("each query must be an object")
    query_id = value.get("id")
    description = value.get("description")
    sql = value.get("sql")
    if not isinstance(query_id, str) or not QUERY_ID.fullmatch(query_id):
        raise QueryCorpusError("query ID must use lowercase letters, digits, and underscores")
    if not isinstance(description, str) or not description.strip():
        raise QueryCorpusError(f"query description must be non-empty: {query_id}")
    if not isinstance(sql, str) or not sql.strip():
        raise QueryCorpusError(f"query SQL must be non-empty: {query_id}")
    normalized = sql.strip()
    if ";" in normalized or normalized.split(None, 1)[0].upper() not in {"SELECT", "WITH"}:
        raise QueryCorpusError(f"query must be a single read-only SELECT or WITH: {query_id}")
    return QueryDefinition(query_id, description.strip(), normalized)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
