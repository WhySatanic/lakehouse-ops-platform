from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any, BinaryIO

from lakehouse_ops.metadata_db_recovery import (
    REQUIRED_TABLES,
    run_metadata_db_recovery,
    write_metadata_db_recovery_report,
)
from lakehouse_ops.trino import TrinoClient


def container_state(service: str) -> tuple[str, bool]:
    container_id = subprocess.check_output(
        ["docker", "compose", "ps", "--all", "-q", service], text=True
    ).strip()
    if not container_id:
        raise RuntimeError(f"Compose container not found: {service}")
    raw = subprocess.check_output(
        ["docker", "inspect", "--format", "{{json .State}}", container_id],
        text=True,
    )
    state = json.loads(raw)
    return container_id, state.get("Running") is True


def service_state() -> dict[str, Any]:
    metastore_id, metastore_running = container_state("hive-metastore")
    database_id, database_running = container_state("metastore-db")
    return {
        "metastore_container_id": metastore_id,
        "metastore_running": metastore_running,
        "database_container_id": database_id,
        "database_running": database_running,
    }


def stop_metastore() -> None:
    subprocess.run(
        ["docker", "compose", "stop", "hive-metastore"],
        check=True,
        timeout=90,
    )


def start_metastore() -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "catalog",
            "up",
            "-d",
            "--wait",
            "--wait-timeout",
            "180",
            "hive-metastore",
        ],
        check=True,
        timeout=210,
    )


def psql(sql: str) -> str:
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "metastore-db",
        "sh",
        "-ec",
        'exec psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" '
        '--tuples-only --no-align --set ON_ERROR_STOP=1 --command "$1"',
        "psql",
        sql,
    ]
    return subprocess.check_output(command, text=True, timeout=90).strip()


def capture_catalog_manifest() -> dict[str, Any]:
    schema_version = psql('SELECT "SCHEMA_VERSION" FROM "VERSION" LIMIT 1')
    entries = psql(
        """
        SELECT lower(d."NAME") || '.' || lower(t."TBL_NAME") || '|' ||
               coalesce(s."LOCATION", '')
        FROM "TBLS" t
        JOIN "DBS" d ON d."DB_ID" = t."DB_ID"
        JOIN "SDS" s ON s."SD_ID" = t."SD_ID"
        ORDER BY 1
        """
    ).splitlines()
    required_tables = psql(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN ('DBS', 'SDS', 'SERDES', 'TBLS', 'VERSION')
        ORDER BY table_name
        """
    ).splitlines()
    payload = "\n".join(entries) + "\n"
    return {
        "metastore_schema_version": schema_version,
        "entry_count": len(entries),
        "entries_sha256": hashlib.sha256(payload.encode()).hexdigest(),
        "required_tables": required_tables,
    }


def run_database_tool(
    shell_command: str,
    *,
    stdin: BinaryIO | None = None,
    stdout: BinaryIO | int | None = subprocess.PIPE,
) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "metastore-db",
            "sh",
            "-ec",
            shell_command,
        ],
        stdin=stdin,
        stdout=stdout,
        stderr=subprocess.PIPE,
        timeout=180,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace")
        raise RuntimeError(f"PostgreSQL backup command failed: {stderr.strip()}")
    return result


def backup_database(path: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary.open("wb") as output:
            run_database_tool(
                'exec pg_dump --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" '
                "--format=custom --no-owner --no-privileges",
                stdout=output,
            )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)

    with path.open("rb") as source:
        toc = run_database_tool("exec pg_restore --list", stdin=source).stdout.decode()
    toc_lines = [line for line in toc.splitlines() if line and not line.startswith(";")]
    required_tables = sorted(
        table
        for table in REQUIRED_TABLES
        if re.search(rf"\bTABLE public {re.escape(table)}\b", toc)
    )
    return {
        "format": "postgresql-custom",
        "file_name": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": file_sha256(path),
        "toc_entries": len(toc_lines),
        "required_tables": required_tables,
    }


def inject_catalog_loss() -> None:
    psql(
        "DROP SCHEMA public CASCADE; "
        "CREATE SCHEMA public AUTHORIZATION CURRENT_USER;"
    )


def inspect_catalog_loss() -> dict[str, Any]:
    count = psql(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN ('DBS', 'SDS', 'SERDES', 'TBLS', 'VERSION')
        """
    )
    return {"core_table_count": int(count)}


def restore_database(path: Path, expected_sha256: str) -> None:
    actual_sha256 = file_sha256(path)
    if actual_sha256 != expected_sha256:
        raise RuntimeError("metastore backup checksum changed before restore")
    with path.open("rb") as source:
        run_database_tool(
            'exec pg_restore --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" '
            "--clean --if-exists --no-owner --no-privileges --exit-on-error",
            stdin=source,
        )
    psql("ANALYZE")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("server")
    parser.add_argument("report", type=Path)
    parser.add_argument("backup", type=Path)
    args = parser.parse_args()

    backup_evidence: dict[str, Any] = {}

    def create_backup() -> dict[str, Any]:
        backup_evidence.update(backup_database(args.backup))
        return dict(backup_evidence)

    def restore_backup() -> None:
        restore_database(args.backup, backup_evidence["sha256"])

    report = run_metadata_db_recovery(
        lambda: TrinoClient(args.server, user="lakehouse-recovery-drill", timeout=60),
        capture_catalog_manifest,
        service_state,
        stop_metastore,
        create_backup,
        inject_catalog_loss,
        inspect_catalog_loss,
        restore_backup,
        start_metastore,
    )
    write_metadata_db_recovery_report(args.report, report)
    print(args.report.read_text(encoding="utf-8").strip())


if __name__ == "__main__":
    main()
