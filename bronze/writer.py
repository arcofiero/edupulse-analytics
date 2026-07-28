from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from schemas.contracts import validate_student_event, validation_error_type


def parse_event_date(payload: dict[str, Any]) -> str:
    event_ts = payload.get("event_ts")
    if not isinstance(event_ts, str):
        return "unknown"
    try:
        parsed = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
    except ValueError:
        return "unknown"
    return parsed.astimezone(timezone.utc).date().isoformat()


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(payload, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as input_file:
        return [json.loads(line) for line in input_file if line.strip()]


@dataclass
class BronzeWriteResult:
    accepted: int = 0
    dlq: int = 0


class BronzeWriter:
    def __init__(self, root_path: Path) -> None:
        self.root_path = root_path

    def write_events(self, events: Iterable[dict[str, Any]]) -> BronzeWriteResult:
        result = BronzeWriteResult()
        for event in events:
            errors = validate_student_event(event)
            if errors or event.get("is_malformed"):
                self._write_dlq(event, errors)
                result.dlq += 1
                continue

            event_date = parse_event_date(event)
            year_cohort = event["year_cohort"]
            table = "bronze_offline_events" if event["source"] == "campus" else "bronze_student_events"
            output_path = (
                self.root_path
                / table
                / f"event_date={event_date}"
                / f"year_cohort={year_cohort}"
                / "events.jsonl"
            )
            append_jsonl(output_path, event)
            result.accepted += 1
        return result

    def _write_dlq(self, event: dict[str, Any], errors: list[str]) -> None:
        event_date = parse_event_date(event)
        error_type = event.get("error_type") or validation_error_type(errors)
        output_path = (
            self.root_path
            / "bronze_dlq_audit"
            / f"event_date={event_date}"
            / f"error_type={error_type}"
            / "events.jsonl"
        )
        append_jsonl(
            output_path,
            {
                "event_id": event.get("event_id"),
                "event_ts": event.get("event_ts"),
                "produced_ts": event.get("produced_ts"),
                "student_id": event.get("student_id"),
                "event_type": event.get("event_type"),
                "course_id": event.get("course_id"),
                "year_cohort": event.get("year_cohort"),
                "source": event.get("source"),
                "error_type": error_type,
                "validation_errors": errors,
                "raw_payload": json.dumps(event, sort_keys=True),
            },
        )
