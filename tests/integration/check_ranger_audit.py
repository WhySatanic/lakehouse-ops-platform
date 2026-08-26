from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def validate(payload: dict[str, Any]) -> dict[str, Any]:
    response = payload.get("response", {})
    documents = response.get("docs", [])
    if not isinstance(documents, list) or not documents:
        raise ValueError("Ranger audit contains no Trino events")

    results = {document.get("result") for document in documents}
    if 1 not in results:
        raise ValueError("Ranger audit contains no allowed decision")
    if 0 not in results:
        raise ValueError("Ranger audit contains no denied decision")
    if any(document.get("repo") != "lakehouse-trino" for document in documents):
        raise ValueError("Ranger audit contains an unexpected service")
    if any(document.get("enforcer") != "ranger-acl" for document in documents):
        raise ValueError("Ranger audit contains a non-Ranger decision")

    return {
        "status": "ready",
        "service": "lakehouse-trino",
        "events": len(documents),
        "allowed": sum(document.get("result") == 1 for document in documents),
        "denied": sum(document.get("result") == 0 for document in documents),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    print(json.dumps(validate(json.loads(args.report.read_text(encoding="utf-8")))))


if __name__ == "__main__":
    main()
