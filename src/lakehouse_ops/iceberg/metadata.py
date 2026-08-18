from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Protocol


class QueryExecutor(Protocol):
    def query(self, sql: str) -> list[dict[str, Any]]: ...


class MetadataContractError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SnapshotSummary:
    count: int
    current_id: str
    committed_at: str
    operation: str


@dataclass(frozen=True, slots=True)
class FileSummary:
    count: int
    records: int
    total_size_bytes: int
    min_size_bytes: int
    max_size_bytes: int
    delete_file_count: int


@dataclass(frozen=True, slots=True)
class ManifestSummary:
    count: int
    total_size_bytes: int
    added_files: int
    existing_files: int
    deleted_files: int


@dataclass(frozen=True, slots=True)
class PartitionSummary:
    count: int
    records: int
    files: int
    total_size_bytes: int


@dataclass(frozen=True, slots=True)
class IcebergMetadataReport:
    schema_version: str
    status: str
    collected_at: str
    table: str
    snapshots: SnapshotSummary
    files: FileSummary
    manifests: ManifestSummary
    partitions: PartitionSummary

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class IcebergMetadataCollector:
    def __init__(
        self,
        executor: QueryExecutor,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._executor = executor
        self._clock = clock or (lambda: datetime.now(UTC))

    def collect(self, catalog: str, schema: str, table: str) -> IcebergMetadataReport:
        snapshots = _single_row(
            self._executor.query(
                f"""
                SELECT count(*) AS snapshot_count,
                       max_by(snapshot_id, committed_at) AS current_snapshot_id,
                       max(committed_at) AS current_committed_at,
                       max_by(operation, committed_at) AS current_operation
                FROM {_metadata_table(catalog, schema, table, "snapshots")}
                """
            ),
            "snapshots",
        )
        files = _single_row(
            self._executor.query(
                f"""
                SELECT count(*) AS file_count,
                       coalesce(sum(record_count), 0) AS record_count,
                       coalesce(sum(file_size_in_bytes), 0) AS total_size_bytes,
                       coalesce(min(file_size_in_bytes), 0) AS min_size_bytes,
                       coalesce(max(file_size_in_bytes), 0) AS max_size_bytes,
                       count_if(content <> 0) AS delete_file_count
                FROM {_metadata_table(catalog, schema, table, "files")}
                """
            ),
            "files",
        )
        manifests = _single_row(
            self._executor.query(
                f"""
                SELECT count(*) AS manifest_count,
                       coalesce(sum(length), 0) AS total_size_bytes,
                       coalesce(sum(added_data_files_count), 0) AS added_files,
                       coalesce(sum(existing_data_files_count), 0) AS existing_files,
                       coalesce(sum(deleted_data_files_count), 0) AS deleted_files
                FROM {_metadata_table(catalog, schema, table, "manifests")}
                """
            ),
            "manifests",
        )
        partitions = _single_row(
            self._executor.query(
                f"""
                SELECT count(*) AS partition_count,
                       coalesce(sum(record_count), 0) AS record_count,
                       coalesce(sum(file_count), 0) AS file_count,
                       coalesce(sum(total_size), 0) AS total_size_bytes
                FROM {_metadata_table(catalog, schema, table, "partitions")}
                """
            ),
            "partitions",
        )

        snapshot_id = snapshots.get("current_snapshot_id")
        committed_at = snapshots.get("current_committed_at")
        operation = snapshots.get("current_operation")
        if snapshot_id is None or committed_at is None or operation is None:
            raise MetadataContractError("Iceberg table has no current snapshot")

        return IcebergMetadataReport(
            schema_version="1.0",
            status="ready",
            collected_at=self._clock().astimezone(UTC).isoformat(),
            table=".".join((catalog, schema, table)),
            snapshots=SnapshotSummary(
                count=_integer(snapshots, "snapshot_count"),
                current_id=str(snapshot_id),
                committed_at=str(committed_at),
                operation=str(operation),
            ),
            files=FileSummary(
                count=_integer(files, "file_count"),
                records=_integer(files, "record_count"),
                total_size_bytes=_integer(files, "total_size_bytes"),
                min_size_bytes=_integer(files, "min_size_bytes"),
                max_size_bytes=_integer(files, "max_size_bytes"),
                delete_file_count=_integer(files, "delete_file_count"),
            ),
            manifests=ManifestSummary(
                count=_integer(manifests, "manifest_count"),
                total_size_bytes=_integer(manifests, "total_size_bytes"),
                added_files=_integer(manifests, "added_files"),
                existing_files=_integer(manifests, "existing_files"),
                deleted_files=_integer(manifests, "deleted_files"),
            ),
            partitions=PartitionSummary(
                count=_integer(partitions, "partition_count"),
                records=_integer(partitions, "record_count"),
                files=_integer(partitions, "file_count"),
                total_size_bytes=_integer(partitions, "total_size_bytes"),
            ),
        )


def _metadata_table(catalog: str, schema: str, table: str, suffix: str) -> str:
    return ".".join(
        (
            _quote_identifier(catalog),
            _quote_identifier(schema),
            _quote_identifier(f"{table}${suffix}"),
        )
    )


def _quote_identifier(value: str) -> str:
    if not value or "\x00" in value:
        raise ValueError("SQL identifiers must be non-empty and contain no null bytes")
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def _single_row(rows: list[dict[str, Any]], source: str) -> dict[str, Any]:
    if len(rows) != 1:
        raise MetadataContractError(
            f"{source} aggregate returned {len(rows)} rows instead of one"
        )
    return rows[0]


def _integer(row: dict[str, Any], key: str) -> int:
    try:
        value = row[key]
        if isinstance(value, bool):
            raise ValueError
        return int(value)
    except (KeyError, TypeError, ValueError) as error:
        raise MetadataContractError(f"{key} must be an integer") from error
