from __future__ import annotations

import json
import sys
from pathlib import Path

from lakehouse_ops.metadata_db_recovery import (
    MetadataDbRecoveryError,
    validate_metadata_db_recovery_report,
)


def main() -> None:
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    try:
        validate_metadata_db_recovery_report(report)
    except MetadataDbRecoveryError as error:
        raise SystemExit(f"Metadata database recovery evidence failed: {error}") from error
    print(
        json.dumps(
            {
                "status": "ready",
                "incident": report["incident"],
                "backup_sha256": report["backup"]["sha256"],
                "catalog_entries": report["recovery"]["catalog"]["entry_count"],
                "row_count": report["recovery"]["trino"]["row_count"],
                "snapshot_id": report["recovery"]["trino"]["snapshot_id"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
