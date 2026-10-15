"""Read-only inventory of retained S3 object payload, independent of Iceberg references."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def warehouse_inventory(client: Any, *, bucket: str = "lakehouse") -> dict[str, Any]:
    """Enumerate all versions under warehouse/, including keys hidden by delete markers.

    Run with writers and maintenance stopped. Pagination is not a transactional snapshot.
    Key layout classifies files only; it does not establish Iceberg reachability.
    """
    started = datetime.now(UTC).isoformat()
    versioning = client.get_bucket_versioning(Bucket=bucket).get("Status", "Disabled")
    objects = []
    totals = {name: {"objects": 0, "bytes": 0} for name in ("latest", "noncurrent")}
    layout = {name: {"objects": 0, "bytes": 0} for name in ("metadata", "data", "other")}
    markers = []
    seen = set()
    for page in client.get_paginator("list_object_versions").paginate(
        Bucket=bucket, Prefix="warehouse/",
    ):
        for collection in ("Versions", "DeleteMarkers"):
            for entry in page.get(collection, []):
                key, version = entry["Key"], entry["VersionId"]
                identity = (key, version)
                if not key.startswith("warehouse/") or identity in seen:
                    raise ValueError("out-of-scope or duplicate warehouse version")
                seen.add(identity)
                latest = entry["IsLatest"]
                if type(latest) is not bool:
                    raise ValueError("warehouse version requires boolean IsLatest")
                row = {"key": key, "version_id": version, "is_latest": latest}
                if collection == "DeleteMarkers":
                    markers.append(row)
                    continue
                size = entry["Size"]
                if type(size) is not int or size < 0:
                    raise ValueError("warehouse version requires nonnegative integer Size")
                parts = key.split("/")[1:-1]
                category = "metadata" if "metadata" in parts else (
                    "data" if "data" in parts else "other"
                )
                objects.append({**row, "bytes": size, "layout": category})
                for target in (totals["latest" if latest else "noncurrent"], layout[category]):
                    target["objects"] += 1
                    target["bytes"] += size
    return {
        "status": "observed", "bucket": bucket, "prefix": "warehouse/",
        "started_at": started, "finished_at": datetime.now(UTC).isoformat(),
        "bucket_versioning": versioning,
        "scope": "listed_object_version_payload_not_physical_disk_or_billing",
        "iceberg_reachability": "not_classified",
        "consistency": "paginated_listing_requires_quiescent_writers_and_maintenance",
        "total_payload_bytes": sum(row["bytes"] for row in totals.values()),
        "versions": totals, "layout": layout,
        "delete_marker_count": len(markers),
        "objects": sorted(objects, key=lambda row: (row["key"], row["version_id"])),
        "delete_markers": sorted(markers, key=lambda row: (row["key"], row["version_id"])),
    }
