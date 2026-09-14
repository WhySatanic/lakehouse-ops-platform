from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError


class ReportSchemaError(RuntimeError):
    pass


def load_report_schema(schema_path: Path) -> dict[str, Any]:
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
    except (OSError, json.JSONDecodeError, SchemaError) as error:
        raise ReportSchemaError(f"report schema validation failed: {error}") from error
    return schema


def validate_report_schema(report: dict[str, Any], schema_path: Path) -> None:
    schema = load_report_schema(schema_path)
    try:
        Draft202012Validator(schema).validate(report)
    except ValidationError as error:
        raise ReportSchemaError(f"report schema validation failed: {error}") from error
