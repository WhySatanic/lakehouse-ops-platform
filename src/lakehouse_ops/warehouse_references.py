"""Read-only data/delete reference accounting; deliberately produces no orphan verdict."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit


def _files(rows):
    unique = {}
    for row in rows:
        path, size, content = row["file_path"], row["bytes"], row["content"]
        if (not isinstance(path, str) or type(size) is not int or size < 0
                or type(content) is not int or content not in (0, 1, 2)):
            raise ValueError("invalid Iceberg file reference")
        value = {"file_path": path, "bytes": size, "content": content}
        if path in unique and unique[path] != value:
            raise ValueError("conflicting Iceberg references for the same path")
        unique[path] = value
    return [unique[path] for path in sorted(unique)]


def file_references(client: Any, tables: list[dict[str, str]]) -> list[dict[str, Any]]:
    """Inspect selected commerce tables with conservative experiment size bounds."""
    if len(tables) > 32:
        raise ValueError("reference experiment supports at most 32 tables")
    result = []
    for table in tables:
        schema = table["table_schema"].replace('"', '""')
        name = table["table_name"].replace('"', '""')
        identifier = f'lakehouse."{schema}"."{name}'
        snapshots = client.query(f'SELECT snapshot_id FROM {identifier}$snapshots" '
                                 'ORDER BY snapshot_id')
        if len(snapshots) > 100:
            raise ValueError("reference experiment supports at most 100 snapshots per table")
        current = client.query(
            f'SELECT file_path, file_size_in_bytes AS bytes, content FROM {identifier}$files" '
            'LIMIT 100001',
        )
        retained = client.query(
            'SELECT DISTINCT data_file.file_path AS file_path, '
            'data_file.file_size_in_bytes AS bytes, data_file.content AS content '
            f'FROM {identifier}$all_entries" WHERE status IN (0, 1) LIMIT 100001',
        )
        if len(current) > 100000 or len(retained) > 100000:
            raise ValueError("reference experiment supports at most 100000 files per table")
        current, retained = _files(current), _files(retained)
        retained_by_path = {row["file_path"]: row for row in retained}
        if any(retained_by_path.get(row["file_path"]) != row for row in current):
            raise ValueError("current files absent from retained reference union")
        result.append({"table": f"{schema}.{name}", "snapshots": snapshots,
                       "current": current, "retained": retained})
    return result


def _key(path: str, bucket: str) -> str:
    uri = urlsplit(path)
    # Do not guess whether a literal percent denotes an escaped or literal object key.
    if (uri.scheme not in ("s3", "s3a") or uri.netloc != bucket or uri.query or uri.fragment
            or "%" in uri.path or not uri.path.startswith("/warehouse/")):
        raise ValueError("reference is outside the selected plain S3 warehouse URI scope")
    return uri.path[1:]


def reconcile_references(inventory: dict[str, Any], references: list[dict[str, Any]]) -> dict:
    """Partition payload once per S3 version, with current references taking precedence."""
    if inventory["status"] != "observed" or inventory["prefix"] != "warehouse/":
        raise ValueError("requires a complete warehouse version inventory")
    current, retained = {}, {}
    for table in references:
        for name, target in (("current", current), ("retained", retained)):
            for row in table[name]:
                key = _key(row["file_path"], inventory["bucket"])
                value = (row["bytes"], row["content"])
                if key in target and target[key] != value:
                    raise ValueError("conflicting shared file references")
                target[key] = value
    if any(retained.get(key) != value for key, value in current.items()):
        raise ValueError("current files absent from retained reference union")
    latest = {row["key"]: row for row in inventory["objects"] if row["is_latest"]}
    for key, (size, _) in retained.items():
        if key not in latest or latest[key]["bytes"] != size:
            raise ValueError("referenced file missing or size differs from latest S3 version")
    groups = {name: {"versions": 0, "bytes": 0} for name in (
        "current_data", "current_delete", "retained_only_data", "retained_only_delete",
        "latest_not_in_selected_file_references", "s3_noncurrent",
    )}
    for row in inventory["objects"]:
        key = row["key"]
        if not row["is_latest"]:
            group = "s3_noncurrent"
        elif key in current:
            group = "current_data" if current[key][1] == 0 else "current_delete"
        elif key in retained:
            group = "retained_only_data" if retained[key][1] == 0 else "retained_only_delete"
        else:
            group = "latest_not_in_selected_file_references"
        groups[group]["versions"] += 1
        groups[group]["bytes"] += row["bytes"]
    if sum(row["bytes"] for row in groups.values()) != inventory["total_payload_bytes"]:
        raise ValueError("warehouse payload totals do not reconcile")
    return {"status": "reconciled", "scope": "selected_commerce_data_and_delete_references",
            "groups": groups, "total_payload_bytes": inventory["total_payload_bytes"],
            "orphan_classification": "not_performed", "references": references}
