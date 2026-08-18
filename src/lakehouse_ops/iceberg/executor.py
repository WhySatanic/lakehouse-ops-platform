from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Protocol


class SqlExecutor(Protocol):
    def query(self, sql: str) -> list[dict[str, Any]]: ...


class ExecutionContractError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class TableState:
    snapshot_id: str
    snapshot_count: int
    snapshot_ids: tuple[str, ...]
    data_file_count: int
    record_count: int
    manifest_count: int


@dataclass(frozen=True, slots=True)
class MaintenanceExecutionReport:
    schema_version: str
    status: str
    plan_id: str
    action_id: str
    action_type: str
    table: str
    applied: bool
    before: TableState
    after: TableState
    procedure_result: dict[str, int]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class SparkMaintenanceExecutor:
    def __init__(self, executor: SqlExecutor) -> None:
        self._executor = executor

    def run(
        self,
        plan: dict[str, Any],
        action_id: str,
        *,
        apply: bool = False,
        approved_plan_id: str | None = None,
        approved_snapshot_id: str | None = None,
    ) -> MaintenanceExecutionReport:
        plan_id, table, action = _select_action(plan, action_id)
        safety = _object(action, "safety_bounds")
        expected_snapshot = _string(safety, "expected_snapshot_id")
        if safety.get("dry_run_required") is not True:
            raise ExecutionContractError("action must require a dry run")
        if _integer(safety, "max_concurrent_jobs") != 1:
            raise ExecutionContractError("max_concurrent_jobs must equal one")
        before = self._state(table)
        if before.snapshot_id != expected_snapshot:
            raise ExecutionContractError(
                "current snapshot does not match the plan safety bound"
            )
        if not apply:
            return _report("dry_run", plan_id, action, table, False, before, before, {})
        if approved_plan_id != plan_id:
            raise ExecutionContractError("approved plan ID does not match")
        if approved_snapshot_id != expected_snapshot:
            raise ExecutionContractError("approved snapshot ID does not match")

        action_type = _string(action, "action_type")
        procedure_rows = self._execute(action_type, table, action, safety, before)
        if len(procedure_rows) != 1:
            raise ExecutionContractError(f"{action_type} must return exactly one row")
        result = _procedure_result(action_type, procedure_rows[0])
        after = self._state(table)
        status = _reconcile(action_type, action, before, after, result)
        return _report(status, plan_id, action, table, True, before, after, result)

    def _execute(
        self,
        action_type: str,
        table: str,
        action: dict[str, Any],
        safety: dict[str, Any],
        before: TableState,
    ) -> list[dict[str, Any]]:
        if action_type == "rewrite_data_files":
            max_files = _positive_bound(safety, "max_files_to_rewrite")
            parameters = _object(action, "parameters")
            target_size = _integer(parameters, "target_file_size_bytes")
            if target_size <= 0:
                raise ExecutionContractError("target_file_size_bytes must be positive")
            return self._executor.query(
                _rewrite_data_files_sql(table, target_size, max_files)
            )
        if action_type == "rewrite_manifests":
            max_manifests = _positive_bound(safety, "max_manifests_to_rewrite")
            if before.manifest_count > max_manifests:
                raise ExecutionContractError(
                    "manifest count exceeds the rewrite safety bound"
                )
            return self._executor.query(_rewrite_manifests_sql(table))
        parameters = _object(action, "parameters")
        snapshot_ids = _string_array(parameters, "snapshot_ids")
        expected_history = _string_array(safety, "expected_history_snapshot_ids")
        max_snapshots = _positive_bound(safety, "max_snapshots_to_expire")
        if not snapshot_ids or len(snapshot_ids) > max_snapshots:
            raise ExecutionContractError("snapshot expiration batch exceeds safety bound")
        if len(snapshot_ids) != len(set(snapshot_ids)):
            raise ExecutionContractError("snapshot expiration IDs must be unique")
        if set(before.snapshot_ids) != set(expected_history):
            raise ExecutionContractError("snapshot history does not match the approved plan")
        if before.snapshot_id in snapshot_ids:
            raise ExecutionContractError("current snapshot cannot be expired")
        if not set(snapshot_ids).issubset(before.snapshot_ids):
            raise ExecutionContractError("expiration target is absent from live history")
        return self._executor.query(_expire_snapshots_sql(table, snapshot_ids))

    def _state(self, table: str) -> TableState:
        current = self._executor.query(
            f"SELECT snapshot_id FROM {_metadata_table(table, 'history')} "
            "ORDER BY made_current_at DESC LIMIT 1"
        )
        snapshots = self._executor.query(
            f"SELECT snapshot_id FROM {_metadata_table(table, 'snapshots')} "
            "ORDER BY committed_at DESC"
        )
        files = self._executor.query(
            f"SELECT count(*) AS data_file_count, "
            f"coalesce(sum(record_count), 0) AS record_count "
            f"FROM {_metadata_table(table, 'data_files')}"
        )
        manifests = self._executor.query(
            f"SELECT count(*) AS manifest_count "
            f"FROM {_metadata_table(table, 'manifests')}"
        )
        if len(current) != 1 or not snapshots or len(files) != 1 or len(manifests) != 1:
            raise ExecutionContractError("table state queries returned an invalid shape")
        snapshot_ids = tuple(str(row["snapshot_id"]) for row in snapshots)
        current_snapshot_id = str(current[0]["snapshot_id"])
        if current_snapshot_id not in snapshot_ids:
            raise ExecutionContractError("current snapshot is absent from snapshot history")
        return TableState(
            snapshot_id=current_snapshot_id,
            snapshot_count=len(snapshot_ids),
            snapshot_ids=snapshot_ids,
            data_file_count=_integer(files[0], "data_file_count"),
            record_count=_integer(files[0], "record_count"),
            manifest_count=_integer(manifests[0], "manifest_count"),
        )


def _select_action(
    plan: dict[str, Any], action_id: str
) -> tuple[str, str, dict[str, Any]]:
    if not isinstance(plan, dict) or plan.get("schema_version") != "1.0":
        raise ExecutionContractError("unsupported maintenance plan schema_version")
    plan_id = _string(plan, "plan_id")
    table = _string(plan, "table")
    actions = plan.get("actions")
    if not isinstance(actions, (list, tuple)):
        raise ExecutionContractError("actions must be an array")
    matches = [
        action
        for action in actions
        if isinstance(action, dict) and action.get("action_id") == action_id
    ]
    if len(matches) != 1:
        raise ExecutionContractError("action ID must select exactly one action")
    action = matches[0]
    if action.get("action_type") not in {
        "rewrite_data_files",
        "rewrite_manifests",
        "expire_snapshots",
    }:
        raise ExecutionContractError("unsupported action type")
    return plan_id, table, action


def _reconcile(
    action_type: str,
    action: dict[str, Any],
    before: TableState,
    after: TableState,
    result: dict[str, int],
) -> str:
    if before.record_count != after.record_count:
        return "reconciliation_failed"
    if action_type == "expire_snapshots":
        target_ids = _string_array(_object(action, "parameters"), "snapshot_ids")
        expected = set(before.snapshot_ids) - set(target_ids)
        if after.snapshot_id != before.snapshot_id:
            return "reconciliation_failed"
        if before.data_file_count != after.data_file_count:
            return "reconciliation_failed"
        if before.manifest_count != after.manifest_count:
            return "reconciliation_failed"
        if set(after.snapshot_ids) != expected:
            return "reconciliation_failed"
        if after.snapshot_count >= before.snapshot_count:
            return "reconciliation_failed"
        return "succeeded"
    if action_type == "rewrite_manifests":
        if before.data_file_count != after.data_file_count:
            return "reconciliation_failed"
        rewritten = result["rewritten_manifests_count"]
        if rewritten == 0:
            return "noop" if before == after else "reconciliation_failed"
        if after.snapshot_id == before.snapshot_id:
            return "reconciliation_failed"
        if after.manifest_count >= before.manifest_count:
            return "reconciliation_failed"
        if result["added_manifests_count"] >= rewritten:
            return "reconciliation_failed"
        return "succeeded"
    if result["failed_data_files_count"]:
        return "reconciliation_failed"
    rewritten = result["rewritten_data_files_count"]
    if rewritten == 0:
        return "noop" if before == after else "reconciliation_failed"
    if after.snapshot_id == before.snapshot_id:
        return "reconciliation_failed"
    if after.data_file_count >= before.data_file_count:
        return "reconciliation_failed"
    return "succeeded"


def _rewrite_data_files_sql(table: str, target_size: int, max_files: int) -> str:
    catalog, namespace, name = _table_parts(table)
    table_argument = f"{namespace}.{name}".replace("'", "''")
    return (
        f"CALL {_identifier(catalog)}.system.rewrite_data_files("
        f"table => '{table_argument}', options => map("
        f"'target-file-size-bytes', '{target_size}', "
        "'min-input-files', '2', "
        "'max-concurrent-file-group-rewrites', '1', "
        "'partial-progress.enabled', 'false', "
        f"'max-files-to-rewrite', '{max_files}'))"
    )


def _rewrite_manifests_sql(table: str) -> str:
    catalog, namespace, name = _table_parts(table)
    table_argument = f"{namespace}.{name}".replace("'", "''")
    return (
        f"CALL {_identifier(catalog)}.system.rewrite_manifests("
        f"table => '{table_argument}', use_caching => false)"
    )


def _expire_snapshots_sql(table: str, snapshot_ids: tuple[str, ...]) -> str:
    catalog, namespace, name = _table_parts(table)
    table_argument = f"{namespace}.{name}".replace("'", "''")
    snapshot_array = ", ".join(snapshot_ids)
    return (
        f"CALL {_identifier(catalog)}.system.expire_snapshots("
        f"table => '{table_argument}', snapshot_ids => ARRAY({snapshot_array}), "
        "max_concurrent_deletes => 1, stream_results => true, "
        "clean_expired_metadata => false)"
    )


def _procedure_result(action_type: str, row: dict[str, Any]) -> dict[str, int]:
    keys_by_action = {
        "rewrite_manifests": (
            "rewritten_manifests_count",
            "added_manifests_count",
        ),
        "rewrite_data_files": (
            "rewritten_data_files_count",
            "added_data_files_count",
            "rewritten_bytes_count",
            "failed_data_files_count",
        ),
        "expire_snapshots": (
            "deleted_data_files_count",
            "deleted_position_delete_files_count",
            "deleted_equality_delete_files_count",
            "deleted_manifest_files_count",
            "deleted_manifest_lists_count",
            "deleted_statistics_files_count",
        ),
    }
    return {key: _integer(row, key) for key in keys_by_action[action_type]}


def _positive_bound(safety: dict[str, Any], key: str) -> int:
    value = _integer(safety, key)
    if value <= 0:
        raise ExecutionContractError(f"{key} must be positive")
    return value


def _metadata_table(table: str, suffix: str) -> str:
    return ".".join((*(_identifier(part) for part in _table_parts(table)), suffix))


def _table_parts(table: str) -> tuple[str, str, str]:
    parts = table.split(".")
    if len(parts) != 3 or any(not part or "\x00" in part for part in parts):
        raise ExecutionContractError("table must contain catalog, namespace, and name")
    return parts[0], parts[1], parts[2]


def _identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def _object(value: dict[str, Any], key: str) -> dict[str, Any]:
    item = value.get(key)
    if not isinstance(item, dict):
        raise ExecutionContractError(f"{key} must be an object")
    return item


def _string(value: dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ExecutionContractError(f"{key} must be a non-empty string")
    return item


def _integer(value: dict[str, Any], key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int) or item < 0:
        raise ExecutionContractError(f"{key} must be a non-negative integer")
    return item


def _string_array(value: dict[str, Any], key: str) -> tuple[str, ...]:
    items = value.get(key)
    if not isinstance(items, (list, tuple)):
        raise ExecutionContractError(f"{key} must be an array")
    if any(not isinstance(item, str) or not item.isdigit() for item in items):
        raise ExecutionContractError(f"{key} must contain decimal snapshot IDs")
    return tuple(items)


def _report(
    status: str,
    plan_id: str,
    action: dict[str, Any],
    table: str,
    applied: bool,
    before: TableState,
    after: TableState,
    procedure_result: dict[str, int],
) -> MaintenanceExecutionReport:
    return MaintenanceExecutionReport(
        schema_version="1.0",
        status=status,
        plan_id=plan_id,
        action_id=_string(action, "action_id"),
        action_type=_string(action, "action_type"),
        table=table,
        applied=applied,
        before=before,
        after=after,
        procedure_result=procedure_result,
    )
