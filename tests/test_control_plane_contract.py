from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from lakehouse_ops.cli import build_parser
from lakehouse_ops.control_plane_contract import (
    ControlPlaneContractError,
    verify_control_plane_contract,
)

CONTRACT = Path("config/control-plane/contract.json")


def test_repository_contract_matches_public_cli() -> None:
    report = verify_control_plane_contract(CONTRACT, build_parser())

    assert report["status"] == "compatible"
    assert report["commands_verified"] == 16
    assert report["outputs_verified"] == 9
    assert len(report["contract_sha256"]) == 64


def test_contract_digest_is_stable_across_checkout_line_endings(tmp_path: Path) -> None:
    content = CONTRACT.read_text(encoding="utf-8").replace("\r\n", "\n")
    candidate = tmp_path / "contract.json"
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
            lambda contract: contract["outputs"].append(
                dict(contract["outputs"][0])
            ),
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
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    return path
