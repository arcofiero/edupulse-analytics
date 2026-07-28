from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from simulator.models import DLQ_ERROR_TYPES, validate_event_payload

SCHEMA_DIR = Path(__file__).resolve().parent


def load_schema(name: str) -> dict[str, Any]:
    with (SCHEMA_DIR / name).open(encoding="utf-8") as schema_file:
        return json.load(schema_file)


def validate_student_event(payload: dict[str, Any]) -> list[str]:
    errors = validate_event_payload(payload)
    if payload.get("is_malformed"):
        error_type = payload.get("error_type")
        if error_type not in DLQ_ERROR_TYPES:
            errors.append("invalid_dlq_error_type")
    return errors


def validation_error_type(errors: list[str]) -> str:
    if not errors:
        return ""
    first_error = errors[0].split(":", maxsplit=1)[0]
    if first_error in DLQ_ERROR_TYPES:
        return first_error
    return "schema_mismatch"
