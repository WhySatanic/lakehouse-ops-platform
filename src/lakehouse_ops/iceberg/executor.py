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
    data_file_count: int
    record_count: int


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
        max_files = _integer(safety, "max_files_to_rewrite")
        if max_files <= 0:
            raise ExecutionContractError("max_files_to_rewrite must be positive")

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

        parameters = _object(action, "parameters")
        target_size = _integer(parameters, "target_file_size_bytes")
        if target_size <= 0:
            raise ExecutionContractError("target_file_size_bytes must be positive")
        procedure_rows = self._executor.query(
            _rewrite_data_files_sql(table, target_size, max_files)
        )
        if len(procedure_rows) != 1:
            raise ExecutionContractError("rewrite_data_files must return exactly one row")
        result = {
            key: _integer(procedure_rows[0], key)
            for key in (
                "rewritten_data_files_count",
                "added_data_files_count",
                "rewritten_bytes_count",
                "failed_data_files_count",
            )
        }
        after = self._state(table)
        status = _reconcile(before, after, result)
        return _report(status, plan_id, action, table, True, before, after, result)

    def _state(self, table: str) -> TableState:
        snapshots = self._executor.query(
            f"SELECT snapshot_id FROM {_metadata_table(table, 'snapshots')} "
            "ORDER BY committed_at DESC LIMIT 1"
        )
        files = self._executor.query(
            f"SELECT count(*) AS data_file_count, "
            f"coalesce(sum(record_count), 0) AS record_count "
            f"FROM {_metadata_table(table, 'data_files')}"
        )
        if len(snapshots) != 1 or len(files) != 1:
            raise ExecutionContractError("table state queries must return one row")
        return TableState(
            snapshot_id=str(snapshots[0]["snapshot_id"]),
            data_file_count=_integer(files[0], "data_file_count"),
            record_count=_integer(files[0], "record_count"),
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
    if action.get("action_type") != "rewrite_data_files":
        raise ExecutionContractError("unsupported action type")
    return plan_id, table, action


def _reconcile(
    before: TableState, after: TableState, result: dict[str, int]
) -> str:
    if result["failed_data_files_count"]:
        return "reconciliation_failed"
    if before.record_count != after.record_count:
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
