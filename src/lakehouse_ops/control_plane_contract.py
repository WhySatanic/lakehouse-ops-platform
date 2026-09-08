from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from lakehouse_ops.digests import normalized_text_digest
from lakehouse_ops.report_schema import ReportSchemaError, validate_report_schema


class ControlPlaneContractError(RuntimeError):
    pass


def verify_control_plane_contract(
    contract_path: Path, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    try:
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ControlPlaneContractError(f"cannot load control-plane contract: {error}") from error
    if not isinstance(contract, dict) or contract.get("schema_version") != "1.0":
        raise ControlPlaneContractError("unsupported control-plane contract schema_version")
    if contract.get("contract_version") != "1.0.0":
        raise ControlPlaneContractError("control-plane contract_version must be 1.0.0")

    actual = _cli_surface(parser)
    actual_semantics = _cli_option_semantics(parser)
    commands = contract.get("commands")
    if not isinstance(commands, dict) or not commands:
        raise ControlPlaneContractError("control-plane commands must be a non-empty object")
    for command, required_options in commands.items():
        if command not in actual:
            raise ControlPlaneContractError(f"public command was removed: {command}")
        if not isinstance(required_options, list) or not all(
            isinstance(option, str) and option.startswith("--") for option in required_options
        ):
            raise ControlPlaneContractError(f"invalid option contract for {command}")
        missing = sorted(set(required_options) - actual[command])
        if missing:
            raise ControlPlaneContractError(
                f"public options were removed from {command}: {missing}"
            )

    option_semantics = contract.get("option_semantics")
    if not isinstance(option_semantics, dict) or not option_semantics:
        raise ControlPlaneContractError("control-plane option_semantics must be a non-empty object")
    semantics_verified = 0
    for command, options in option_semantics.items():
        if command not in commands:
            raise ControlPlaneContractError(
                f"option semantics reference unknown command: {command}"
            )
        if not isinstance(options, dict) or not options:
            raise ControlPlaneContractError(
                f"option semantics for {command} must be a non-empty object"
            )
        for option, expected in options.items():
            if option not in commands[command]:
                raise ControlPlaneContractError(
                    f"option semantics reference uncontracted option for {command}: {option}"
                )
            _validate_option_semantics(command, option, expected)
            observed = actual_semantics[command][option]
            if expected != observed:
                raise ControlPlaneContractError(
                    f"public option semantics changed for {command} {option}: "
                    f"expected {expected}, observed {observed}"
                )
            semantics_verified += 1

    outputs = contract.get("outputs")
    if not isinstance(outputs, list) or not outputs:
        raise ControlPlaneContractError("control-plane outputs must be a non-empty array")
    names: set[str] = set()
    for output in outputs:
        if not isinstance(output, dict):
            raise ControlPlaneContractError("output contract must be an object")
        name = output.get("name")
        producer = output.get("producer")
        version = output.get("schema_version")
        if not isinstance(name, str) or not name or name in names:
            raise ControlPlaneContractError("output contract names must be unique")
        names.add(name)
        if producer not in commands:
            raise ControlPlaneContractError(f"unknown output producer for {name}: {producer}")
        if not isinstance(version, str) or version.split(".", 1)[0] != "1":
            raise ControlPlaneContractError(f"output {name} must remain on schema major 1")

    report = {
        "schema_version": "1.0",
        "status": "compatible",
        "contract_version": "1.0.0",
        "contract_sha256": normalized_text_digest(contract_path),
        "commands_verified": len(commands),
        "option_semantics_verified": semantics_verified,
        "outputs_verified": len(outputs),
    }
    verifier_output = next(
        (output for output in outputs if output["name"] == "control_plane_contract_verification"),
        None,
    )
    if verifier_output is None:
        raise ControlPlaneContractError(
            "control_plane_contract_verification output contract is required"
        )
    schema_path = verifier_output.get("schema_path")
    if not isinstance(schema_path, str) or not schema_path:
        raise ControlPlaneContractError(
            "control_plane_contract_verification must declare schema_path"
        )
    try:
        validate_report_schema(report, contract_path.parent / schema_path)
    except ReportSchemaError as error:
        raise ControlPlaneContractError(str(error)) from error
    return report


def _cli_surface(parser: argparse.ArgumentParser) -> dict[str, set[str]]:
    subparsers = _subparsers(parser)
    return {
        name: {
            option
            for action in command_parser._actions
            for option in action.option_strings
            if option.startswith("--") and option != "--help"
        }
        for name, command_parser in subparsers.choices.items()
    }


def _cli_option_semantics(
    parser: argparse.ArgumentParser,
) -> dict[str, dict[str, dict[str, Any]]]:
    subparsers = _subparsers(parser)
    return {
        name: {
            option: _action_semantics(action)
            for action in command_parser._actions
            for option in action.option_strings
            if option.startswith("--") and option != "--help"
        }
        for name, command_parser in subparsers.choices.items()
    }


def _subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    return next(
        action for action in parser._actions if isinstance(action, argparse._SubParsersAction)
    )


def _action_semantics(action: argparse.Action) -> dict[str, Any]:
    if isinstance(action, (argparse._StoreTrueAction, argparse._StoreFalseAction)):
        value_type = "boolean"
    elif action.type is Path:
        value_type = "path"
    elif action.type is None:
        value_type = "str"
    else:
        value_type = action.type.__name__
    default = action.default.as_posix() if isinstance(action.default, Path) else action.default
    return {
        "required": action.required,
        "type": value_type,
        "default": default,
        "choices": list(action.choices) if action.choices is not None else [],
    }


def _validate_option_semantics(command: str, option: str, semantics: object) -> None:
    required_keys = {"required", "type", "default", "choices"}
    if not isinstance(option, str) or not option.startswith("--"):
        raise ControlPlaneContractError(f"invalid semantic option for {command}")
    if not isinstance(semantics, dict) or set(semantics) != required_keys:
        raise ControlPlaneContractError(f"invalid option semantics for {command} {option}")
    if not isinstance(semantics["required"], bool):
        raise ControlPlaneContractError(f"invalid required flag for {command} {option}")
    if semantics["type"] not in {"boolean", "float", "int", "path", "str"}:
        raise ControlPlaneContractError(f"invalid option type for {command} {option}")
    if not isinstance(semantics["choices"], list):
        raise ControlPlaneContractError(f"invalid option choices for {command} {option}")
