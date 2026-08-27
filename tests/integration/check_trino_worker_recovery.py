from __future__ import annotations

import json
import sys
from pathlib import Path

from lakehouse_ops.trino_worker_recovery import (
    TrinoWorkerRecoveryError,
    validate_trino_worker_recovery_report,
)


def main() -> None:
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    try:
        validate_trino_worker_recovery_report(report)
    except TrinoWorkerRecoveryError as error:
        raise SystemExit(f"Trino worker recovery evidence failed: {error}") from error
    print(
        json.dumps(
            {
                "status": "ready",
                "incident": report["incident"],
                "in_flight_error": report["in_flight_query"]["error_name"],
                "row_count": report["restored"]["row_count"],
                "snapshot_id": report["restored"]["snapshot_id"],
                "active_workers": report["restored"]["topology"]["active_workers"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
