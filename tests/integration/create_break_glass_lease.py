from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--state", choices=("active", "expired"), required=True)
    args = parser.parse_args()

    now = datetime.now(UTC).replace(microsecond=0)
    if args.state == "active":
        issued_at = now - timedelta(minutes=5)
        expires_at = now + timedelta(minutes=20)
    else:
        issued_at = now - timedelta(minutes=30)
        expires_at = now - timedelta(minutes=1)
    lease = {
        "schema_version": "1.0",
        "grant_id": "BG-CI-1",
        "user": "incident-responder",
        "role": "platform_admin",
        "approved_by": "incident-commander",
        "ticket": "INC-CI-1",
        "reason": "exercise emergency lakehouse recovery access",
        "issued_at": issued_at.isoformat().replace("+00:00", "Z"),
        "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
    }
    args.output.write_text(json.dumps(lease, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "created", "state": args.state, "path": str(args.output)}))


if __name__ == "__main__":
    main()
