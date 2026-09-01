from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from lakehouse_ops.digests import normalized_text_digest


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

    return {
        "schema_version": "1.0",
        "status": "compatible",
        "contract_version": "1.0.0",
        "contract_sha256": normalized_text_digest(contract_path),
        "commands_verified": len(commands),
        "outputs_verified": len(outputs),
    }


def _cli_surface(parser: argparse.ArgumentParser) -> dict[str, set[str]]:
    subparsers = next(
        action
        for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    return {
        name: {
            option
            for action in command_parser._actions
            for option in action.option_strings
            if option.startswith("--") and option != "--help"
        }
        for name, command_parser in subparsers.choices.items()
    }
