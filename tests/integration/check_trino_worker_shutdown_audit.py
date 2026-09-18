from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any


def validate(shutdown: dict[str, Any], audit: dict[str, Any]) -> dict[str, Any]:
    security = shutdown.get("security", {})
    expected_users = {
        security.get("query_user"),
        security.get("operator_user"),
    }
    if expected_users != {"platform_admin", "lakehouse-operator"}:
        raise ValueError("shutdown report identities are unexpected")
    started_at = _timestamp(shutdown.get("audit_window", {}).get("started_at"))

    documents = audit.get("response", {}).get("docs", [])
    if not isinstance(documents, list):
        raise ValueError("Ranger audit documents are invalid")
    allowed_users = {
        document.get("reqUser")
        for document in documents
        if document.get("repo") == "lakehouse-trino"
        and document.get("enforcer") == "ranger-acl"
        and document.get("result") == 1
        and _timestamp(document.get("evtTime")) >= started_at
    }
    missing = expected_users - allowed_users
    if missing:
        raise ValueError(
            "Ranger audit has no allowed shutdown-drill decision for: "
            + ", ".join(sorted(missing))
        )
    return {
        "status": "ready",
        "query_user": security["query_user"],
        "operator_user": security["operator_user"],
        "allowed_users": sorted(expected_users),
    }


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("Ranger audit timestamp is missing")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("shutdown_report", type=Path)
    parser.add_argument("ranger_audit", type=Path)
    args = parser.parse_args()
    shutdown = json.loads(args.shutdown_report.read_text(encoding="utf-8"))
    audit = json.loads(args.ranger_audit.read_text(encoding="utf-8"))
    print(json.dumps(validate(shutdown, audit), sort_keys=True))


if __name__ == "__main__":
    main()
