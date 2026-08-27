from __future__ import annotations

import json
import sys
from pathlib import Path

from lakehouse_ops.metastore_recovery import (
    MetastoreRecoveryError,
    validate_metastore_recovery_report,
)


def main() -> None:
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    try:
        validate_metastore_recovery_report(report)
    except MetastoreRecoveryError as error:
        raise SystemExit(f"Hive Metastore recovery evidence failed: {error}") from error
    print(
        json.dumps(
            {
                "status": "ready",
                "incident": report["incident"],
                "row_count": report["recovery"]["row_count"],
                "snapshot_id": report["recovery"]["snapshot_id"],
                "metadata_db_container_preserved": True,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
