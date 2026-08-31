from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).parents[1]
CHECKER_PATH = ROOT / "tests" / "integration" / "check_ranger_admin.py"
SPEC = importlib.util.spec_from_file_location("check_ranger_admin", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


def valid_service_definition() -> dict[str, object]:
    return {
        "name": "trino",
        "implClass": "org.apache.ranger.services.trino.RangerServiceTrino",
        "resources": [{"name": name} for name in CHECKER.REQUIRED_RESOURCES],
        "accessTypes": [{"name": name} for name in CHECKER.REQUIRED_ACCESS_TYPES],
        "dataMaskDef": {"maskTypes": [{"name": "MASK_NULL"}]},
        "rowFilterDef": {
            "resources": [
                {"name": name} for name in ("catalog", "schema", "table")
            ]
        },
    }


def test_validate_accepts_trino_service_definition() -> None:
    assert CHECKER.validate_service_definition(valid_service_definition()) == []


def test_validate_reports_identity_and_resource_contract_gaps() -> None:
    document = valid_service_definition()
    document["name"] = "hive"
    document["implClass"] = "wrong"
    document["resources"] = []
    document["accessTypes"] = []

    assert CHECKER.validate_service_definition(document) == [
        "name",
        "implClass",
        "resources.coverage",
        "accessTypes.coverage",
    ]


def test_validate_rejects_non_array_contract_sections() -> None:
    document = valid_service_definition()
    document["resources"] = None
    document["accessTypes"] = None

    assert CHECKER.validate_service_definition(document) == ["resources", "accessTypes"]


def test_validate_reports_missing_mask_and_filter_capabilities() -> None:
    document = valid_service_definition()
    document["dataMaskDef"] = {"maskTypes": []}
    document["rowFilterDef"] = {"resources": []}

    assert CHECKER.validate_service_definition(document) == [
        "dataMaskDef.maskTypes",
        "rowFilterDef.resources",
    ]
