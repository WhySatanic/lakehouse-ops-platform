from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from pathlib import Path

import pytest

from lakehouse_ops import control_plane_contract as contract_module
from lakehouse_ops.cli import build_parser
from lakehouse_ops.control_plane_contract import (
    ControlPlaneContractError,
    verify_control_plane_contract,
)
from lakehouse_ops.digests import normalized_text_digest

CONTRACT = Path("config/control-plane/contract.json")


def test_repository_contract_matches_public_cli() -> None:
    report = verify_control_plane_contract(CONTRACT, build_parser())

    assert report["status"] == "compatible"
    assert report["commands_verified"] == 18
    assert report["option_semantics_verified"] == 40
    assert report["outputs_verified"] == 12
    assert len(report["contract_sha256"]) == 64


def test_contract_digest_is_stable_across_checkout_line_endings(tmp_path: Path) -> None:
    content = CONTRACT.read_text(encoding="utf-8").replace("\r\n", "\n")
    candidate = tmp_path / "contract.json"
    _copy_schemas(tmp_path)
    candidate.write_bytes(content.encode())
    lf_report = verify_control_plane_contract(candidate, build_parser())
    candidate.write_bytes(content.replace("\n", "\r\n").encode())

    crlf_report = verify_control_plane_contract(candidate, build_parser())

    assert crlf_report["contract_sha256"] == lf_report["contract_sha256"]


def test_removed_command_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    contract["commands"]["removed-command"] = []

    with pytest.raises(ControlPlaneContractError, match="public command was removed"):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


def test_removed_option_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    contract["commands"]["doctor"].append("--removed-option")

    with pytest.raises(ControlPlaneContractError, match="public options were removed"):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("required", True),
        ("type", "int"),
        ("default", 8),
        ("choices", ["file"]),
    ],
)
def test_changed_option_semantics_are_rejected(tmp_path: Path, field: str, value: object) -> None:
    contract = _load_contract()
    semantics = contract["option_semantics"]["ingest-weather"]["--backend"]
    semantics[field] = value

    with pytest.raises(ControlPlaneContractError, match="option semantics changed"):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


def test_uncontracted_semantic_option_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    contract["option_semantics"]["audit-landing"]["--missing"] = {
        "required": False,
        "type": "str",
        "default": None,
        "choices": [],
    }

    with pytest.raises(ControlPlaneContractError, match="uncontracted option"):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


def test_malformed_option_semantics_are_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    del contract["option_semantics"]["doctor"]["--backend"]["choices"]

    with pytest.raises(ControlPlaneContractError, match="invalid option semantics"):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


def test_incompatible_output_major_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    contract["outputs"][0]["schema_version"] = "2.0"

    with pytest.raises(ControlPlaneContractError, match="must remain on schema major 1"):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


def test_unknown_output_producer_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    contract["outputs"][0]["producer"] = "missing-producer"

    with pytest.raises(ControlPlaneContractError, match="unknown output producer"):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


def test_output_without_schema_path_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    del contract["outputs"][0]["schema_path"]

    with pytest.raises(ControlPlaneContractError, match="must declare schema_path"):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


def test_missing_output_schema_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    contract["outputs"][0]["schema_path"] = "schemas/missing.schema.json"

    with pytest.raises(ControlPlaneContractError, match="schema_path does not exist"):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


def test_output_without_schema_digest_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    contract["outputs"][0].pop("schema_sha256", None)

    with pytest.raises(ControlPlaneContractError, match="must declare schema_sha256"):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


@pytest.mark.parametrize("digest", ["invalid", "0" * 64])
def test_output_schema_digest_drift_is_rejected(tmp_path: Path, digest: str) -> None:
    contract = _load_contract()
    contract["outputs"][0]["schema_sha256"] = digest

    with pytest.raises(ControlPlaneContractError, match="schema_sha256"):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


def test_refresh_output_schema_digests_repairs_contract_atomically(tmp_path: Path) -> None:
    contract = _load_contract()
    contract["outputs"][0]["schema_sha256"] = "0" * 64
    path = _write_contract(tmp_path, contract)

    report = contract_module.refresh_control_plane_schema_digests(path, build_parser())

    updated = json.loads(path.read_text(encoding="utf-8"))
    schema_path = path.parent / updated["outputs"][0]["schema_path"]
    assert report["status"] == "compatible"
    assert report["outputs_verified"] == 12
    assert updated["outputs"][0]["schema_sha256"] == normalized_text_digest(schema_path)
    assert list(tmp_path.glob(".lakeops-contract-*.tmp")) == []


def test_refresh_rejects_invalid_candidate_without_replacing_contract(tmp_path: Path) -> None:
    path = _write_contract(tmp_path, _load_contract())
    original = path.read_bytes()
    schema_path = tmp_path / "schemas" / "iceberg-metadata-report.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema["type"] = "invalid"
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    with pytest.raises(ControlPlaneContractError, match="invalid output schema"):
        contract_module.refresh_control_plane_schema_digests(path, build_parser())

    assert path.read_bytes() == original
    assert list(tmp_path.glob(".lakeops-contract-*.tmp")) == []


def test_refresh_preserves_contract_when_atomic_replace_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contract = _load_contract()
    contract["outputs"][0]["schema_sha256"] = "0" * 64
    path = _write_contract(tmp_path, contract)
    original = path.read_bytes()

    def fail_replace(source: Path, destination: Path) -> None:
        raise OSError("replace denied")

    monkeypatch.setattr(contract_module.os, "replace", fail_replace)

    with pytest.raises(ControlPlaneContractError, match=r"cannot update.*replace denied"):
        contract_module.refresh_control_plane_schema_digests(path, build_parser())

    assert path.read_bytes() == original
    assert list(tmp_path.glob(".lakeops-contract-*.tmp")) == []


def test_absolute_output_schema_path_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    schema_path = (CONTRACT.parent / contract["outputs"][0]["schema_path"]).resolve()
    contract["outputs"][0]["schema_path"] = str(schema_path)

    with pytest.raises(ControlPlaneContractError, match="must stay within contract directory"):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


def test_output_schema_path_escape_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    contract_dir = tmp_path / "contract"
    external_schema = tmp_path / "external.schema.json"
    source_schema = CONTRACT.parent / contract["outputs"][0]["schema_path"]
    shutil.copyfile(source_schema, external_schema)
    contract["outputs"][0]["schema_path"] = "../external.schema.json"

    with pytest.raises(ControlPlaneContractError, match="must stay within contract directory"):
        verify_control_plane_contract(_write_contract(contract_dir, contract), build_parser())


def test_invalid_output_schema_definition_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    schema_path = tmp_path / contract["outputs"][0]["schema_path"]
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema["type"] = "invalid"
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    with pytest.raises(ControlPlaneContractError, match="invalid output schema"):
        verify_control_plane_contract(path, build_parser())


def test_output_schema_version_mismatch_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    schema_path = tmp_path / contract["outputs"][0]["schema_path"]
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema["properties"]["schema_version"]["const"] = "1.1"
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    with pytest.raises(ControlPlaneContractError, match="schema_version does not match"):
        verify_control_plane_contract(path, build_parser())


def test_optional_output_schema_version_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    schema_path = tmp_path / contract["outputs"][0]["schema_path"]
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema["required"].remove("schema_version")
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    with pytest.raises(ControlPlaneContractError, match="must require schema_version"):
        verify_control_plane_contract(path, build_parser())


def test_output_schema_without_draft_2020_12_dialect_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    schema_path = tmp_path / contract["outputs"][0]["schema_path"]
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema["$schema"] = "http://json-schema.org/draft-07/schema#"
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    with pytest.raises(ControlPlaneContractError, match="must declare Draft 2020-12"):
        verify_control_plane_contract(path, build_parser())


def test_output_schema_without_id_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    schema_path = tmp_path / contract["outputs"][0]["schema_path"]
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    del schema["$id"]
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    with pytest.raises(ControlPlaneContractError, match="must declare a schema id"):
        verify_control_plane_contract(path, build_parser())


def test_duplicate_output_schema_id_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    first_path = tmp_path / contract["outputs"][0]["schema_path"]
    second_path = tmp_path / contract["outputs"][1]["schema_path"]
    first_schema = json.loads(first_path.read_text(encoding="utf-8"))
    second_schema = json.loads(second_path.read_text(encoding="utf-8"))
    second_schema["$id"] = first_schema["$id"]
    second_path.write_text(json.dumps(second_schema), encoding="utf-8")

    with pytest.raises(ControlPlaneContractError, match="schema ids must be unique"):
        verify_control_plane_contract(path, build_parser())


@pytest.mark.parametrize("keyword", ["$ref", "$dynamicRef"])
def test_external_output_schema_reference_is_rejected(
    tmp_path: Path, keyword: str
) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    schema_path = tmp_path / contract["outputs"][0]["schema_path"]
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema["properties"]["schema_version"][keyword] = (
        "https://schemas.example/schema-version.json"
    )
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    with pytest.raises(ControlPlaneContractError, match="external schema reference"):
        verify_control_plane_contract(path, build_parser())


def test_local_output_schema_anchor_is_resolved(tmp_path: Path) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    schema_path = tmp_path / contract["outputs"][0]["schema_path"]
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema.setdefault("$defs", {})["declared_version"] = {
        "$anchor": "declaredVersion",
        "type": "string",
    }
    schema["properties"]["schema_version"]["$ref"] = "#declaredVersion"
    _write_schema(path, contract, schema_path, schema)

    report = verify_control_plane_contract(path, build_parser())

    assert report["status"] == "compatible"


@pytest.mark.parametrize(
    ("first_keyword", "second_keyword"),
    [
        ("$anchor", "$anchor"),
        ("$dynamicAnchor", "$dynamicAnchor"),
        ("$anchor", "$dynamicAnchor"),
    ],
)
def test_duplicate_local_output_schema_anchor_is_rejected(
    tmp_path: Path,
    first_keyword: str,
    second_keyword: str,
) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    schema_path = tmp_path / contract["outputs"][0]["schema_path"]
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema.setdefault("$defs", {}).update(
        {
            "first_anchor": {first_keyword: "duplicateAnchor", "type": "string"},
            "second_anchor": {second_keyword: "duplicateAnchor", "type": "string"},
        }
    )
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    with pytest.raises(ControlPlaneContractError, match="duplicate schema anchor"):
        verify_control_plane_contract(path, build_parser())


def test_duplicate_local_output_schema_resource_id_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    schema_path = tmp_path / contract["outputs"][0]["schema_path"]
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema.setdefault("$defs", {}).update(
        {
            "first_resource": {"$id": "shared", "type": "string"},
            "second_resource": {"$id": "shared", "type": "string"},
        }
    )
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    with pytest.raises(ControlPlaneContractError, match="duplicate schema resource id"):
        verify_control_plane_contract(path, build_parser())


def test_relative_output_schema_resource_ids_use_parent_scope(tmp_path: Path) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    schema_path = tmp_path / contract["outputs"][0]["schema_path"]
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema.setdefault("$defs", {}).update(
        {
            "first_parent": {
                "$id": "first/",
                "$defs": {"child": {"$id": "shared", "type": "string"}},
            },
            "second_parent": {
                "$id": "second/",
                "$defs": {"child": {"$id": "shared", "type": "string"}},
            },
        }
    )
    _write_schema(path, contract, schema_path, schema)

    report = verify_control_plane_contract(path, build_parser())

    assert report["status"] == "compatible"


@pytest.mark.parametrize("keyword", ["$ref", "$dynamicRef"])
def test_unresolved_local_output_schema_reference_is_rejected(
    tmp_path: Path, keyword: str
) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    schema_path = tmp_path / contract["outputs"][0]["schema_path"]
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema["properties"]["schema_version"][keyword] = "#/$defs/missing"
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    with pytest.raises(ControlPlaneContractError, match="unresolved schema reference"):
        verify_control_plane_contract(path, build_parser())


def test_report_schema_drift_is_rejected(tmp_path: Path) -> None:
    contract = _load_contract()
    path = _write_contract(tmp_path, contract)
    schema_path = tmp_path / "schemas" / "control-plane-contract-verification.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema["properties"]["status"] = {"const": "broken"}
    _write_schema(path, contract, schema_path, schema)

    with pytest.raises(ControlPlaneContractError, match="report schema validation failed"):
        verify_control_plane_contract(path, build_parser())


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda contract: contract.update(schema_version="2.0"), "unsupported"),
        (lambda contract: contract.update(contract_version="1.1.0"), "must be 1.0.0"),
        (lambda contract: contract.update(commands={}), "non-empty object"),
        (
            lambda contract: contract["commands"].update(doctor=["output"]),
            "invalid option contract",
        ),
        (lambda contract: contract.update(outputs=[]), "non-empty array"),
        (
            lambda contract: contract["outputs"].append(dict(contract["outputs"][0])),
            "names must be unique",
        ),
    ],
)
def test_malformed_contract_is_rejected(
    tmp_path: Path, mutation: Callable[[dict[str, object]], object], message: str
) -> None:
    contract = _load_contract()
    mutation(contract)

    with pytest.raises(ControlPlaneContractError, match=message):
        verify_control_plane_contract(_write_contract(tmp_path, contract), build_parser())


def test_invalid_json_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "contract.json"
    path.write_text("{", encoding="utf-8")

    with pytest.raises(ControlPlaneContractError, match="cannot load"):
        verify_control_plane_contract(path, build_parser())


def _load_contract() -> dict[str, object]:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def _write_contract(tmp_path: Path, contract: dict[str, object]) -> Path:
    _copy_schemas(tmp_path)
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    return path


def _copy_schemas(tmp_path: Path) -> None:
    target = tmp_path / "schemas"
    if not target.exists():
        shutil.copytree(CONTRACT.parent / "schemas", target)


def _write_schema(
    contract_path: Path,
    contract: dict[str, object],
    schema_path: Path,
    schema: dict[str, object],
) -> None:
    schema_path.write_text(json.dumps(schema), encoding="utf-8")
    relative_path = schema_path.relative_to(contract_path.parent).as_posix()
    output = next(
        output
        for output in contract["outputs"]
        if output["schema_path"] == relative_path
    )
    output["schema_sha256"] = normalized_text_digest(schema_path)
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
