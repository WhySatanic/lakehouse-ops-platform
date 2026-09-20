from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any


def validate(drill: dict[str, Any], audit: dict[str, Any]) -> dict[str, Any]:
    user = drill.get("user")
    if user != "incident-responder":
        raise ValueError("break-glass drill identity is unexpected")
    started_at = _timestamp(drill.get("audit_window", {}).get("started_at"))
    documents = audit.get("response", {}).get("docs", [])
    if not isinstance(documents, list):
        raise ValueError("Ranger audit documents are invalid")

    results = {
        document.get("result")
        for document in documents
        if document.get("repo") == "lakehouse-trino"
        and document.get("enforcer") == "ranger-acl"
        and document.get("reqUser") == user
        and _timestamp(document.get("evtTime")) >= started_at
    }
    missing = {0, 1} - results
    if missing:
        names = {0: "denied", 1: "allowed"}
        raise ValueError(
            "Ranger audit has no break-glass decision for: "
            + ", ".join(names[result] for result in sorted(missing))
        )
    return {
        "status": "ready",
        "user": user,
        "allowed": True,
        "denied": True,
        "security": drill["security"],
    }


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("Ranger audit timestamp is missing")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("drill_report", type=Path)
    parser.add_argument("ranger_audit", type=Path)
    args = parser.parse_args()
    drill = json.loads(args.drill_report.read_text(encoding="utf-8"))
    audit = json.loads(args.ranger_audit.read_text(encoding="utf-8"))
    print(json.dumps(validate(drill, audit), sort_keys=True))


if __name__ == "__main__":
    main()
