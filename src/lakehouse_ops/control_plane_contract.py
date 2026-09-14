from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from referencing import Registry, Resource
from referencing.exceptions import Unresolvable

from lakehouse_ops.digests import normalized_text_digest
from lakehouse_ops.report_schema import (
    ReportSchemaError,
    load_report_schema,
    validate_report_schema,
)


class ControlPlaneContractError(RuntimeError):
    pass


DRAFT_2020_12 = "https://json-schema.org/draft/2020-12/schema"


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
    schema_ids: set[str] = set()
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
        schema_path = output.get("schema_path")
        if not isinstance(schema_path, str) or not schema_path:
            raise ControlPlaneContractError(f"output {name} must declare schema_path")
        resolved_schema_path = _resolve_output_schema_path(contract_path, schema_path, name)
        if not resolved_schema_path.is_file():
            raise ControlPlaneContractError(
                f"output schema_path does not exist for {name}: {schema_path}"
            )
        schema_sha256 = output.get("schema_sha256")
        if not isinstance(schema_sha256, str) or len(schema_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in schema_sha256
        ):
            raise ControlPlaneContractError(
                f"output {name} must declare schema_sha256 as a lowercase SHA-256 digest"
            )
        try:
            schema = load_report_schema(resolved_schema_path)
        except ReportSchemaError as error:
            raise ControlPlaneContractError(
                f"invalid output schema for {name}: {error}"
            ) from error
        schema_id = _validate_output_schema_identity(name, schema)
        if schema_id in schema_ids:
            raise ControlPlaneContractError(f"output schema ids must be unique: {schema_id}")
        schema_ids.add(schema_id)
        _validate_local_schema_references(name, schema)
        _validate_output_schema_version(name, version, schema)
        actual_schema_sha256 = normalized_text_digest(resolved_schema_path)
        if schema_sha256 != actual_schema_sha256:
            raise ControlPlaneContractError(
                f"output {name} schema_sha256 mismatch: "
                f"expected {schema_sha256}, observed {actual_schema_sha256}"
            )

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
    schema_path = verifier_output["schema_path"]
    try:
        validate_report_schema(report, contract_path.parent / schema_path)
    except ReportSchemaError as error:
        raise ControlPlaneContractError(str(error)) from error
    return report


def _resolve_output_schema_path(contract_path: Path, schema_path: str, name: str) -> Path:
    schema_reference = Path(schema_path)
    contract_directory = contract_path.parent.resolve()
    resolved_schema_path = (contract_directory / schema_reference).resolve()
    if schema_reference.is_absolute() or not resolved_schema_path.is_relative_to(
        contract_directory
    ):
        raise ControlPlaneContractError(
            f"output schema_path must stay within contract directory for {name}: {schema_path}"
        )
    return resolved_schema_path


def _validate_output_schema_version(
    name: str, declared_version: str, schema: dict[str, Any]
) -> None:
    properties = schema.get("properties")
    version_schema = properties.get("schema_version") if isinstance(properties, dict) else None
    if not isinstance(version_schema, dict) or version_schema.get("const") != declared_version:
        raise ControlPlaneContractError(
            f"output {name} schema_version does not match its schema const"
        )
    required = schema.get("required")
    if not isinstance(required, list) or "schema_version" not in required:
        raise ControlPlaneContractError(f"output {name} schema must require schema_version")


def _validate_output_schema_identity(name: str, schema: dict[str, Any]) -> str:
    if schema.get("$schema") != DRAFT_2020_12:
        raise ControlPlaneContractError(
            f"output {name} schema must declare Draft 2020-12"
        )
    schema_id = schema.get("$id")
    if not isinstance(schema_id, str) or not schema_id:
        raise ControlPlaneContractError(f"output {name} must declare a schema id")
    return schema_id


def _validate_local_schema_references(name: str, schema: dict[str, Any]) -> None:
    pending: list[object] = [schema]
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            for key, value in node.items():
                if key in {"$ref", "$dynamicRef"} and (
                    not isinstance(value, str) or not value.startswith("#")
                ):
                    raise ControlPlaneContractError(
                        f"output {name} has external schema reference: {value}"
                    )
                pending.append(value)
        elif isinstance(node, list):
            pending.extend(node)

    resource = Resource.from_contents(schema)
    registry = Registry().with_resource(schema["$id"], resource).crawl()
    _validate_schema_resource_references(
        name,
        resource,
        registry.resolver(schema["$id"]),
        schema["$id"],
        {},
        set(),
    )


def _validate_schema_resource_references(
    name: str,
    resource: Resource[Any],
    resolver: Any,
    base_uri: str,
    anchors_by_resource: dict[str, set[str]],
    resource_uris: set[str],
) -> None:
    resource_id = resource.id()
    resource_uri = urljoin(base_uri, resource_id) if resource_id else base_uri
    if resource_id:
        if resource_uri in resource_uris:
            raise ControlPlaneContractError(
                f"output {name} has duplicate schema resource id: {resource_uri}"
            )
        resource_uris.add(resource_uri)
    anchor_names = anchors_by_resource.setdefault(resource_uri, set())
    for anchor in resource.anchors():
        if anchor.name in anchor_names:
            raise ControlPlaneContractError(
                f"output {name} has duplicate schema anchor: {anchor.name}"
            )
        anchor_names.add(anchor.name)

    contents = resource.contents
    if isinstance(contents, dict):
        for keyword in ("$ref", "$dynamicRef"):
            reference = contents.get(keyword)
            if isinstance(reference, str):
                try:
                    resolver.lookup(reference)
                except Unresolvable as error:
                    raise ControlPlaneContractError(
                        f"output {name} has unresolved schema reference: {reference}"
                    ) from error
    for subresource in resource.subresources():
        _validate_schema_resource_references(
            name,
            subresource,
            resolver.in_subresource(subresource),
            resource_uri,
            anchors_by_resource,
            resource_uris,
        )


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
