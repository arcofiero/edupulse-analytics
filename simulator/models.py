from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


PERSONA_DISTRIBUTION: tuple[tuple[str, float], ...] = (
    ("engaged", 0.25),
    ("passive", 0.35),
    ("at_risk", 0.20),
    ("night_owl", 0.12),
    ("ghost", 0.08),
)

ONLINE_EVENT_TYPES: tuple[str, ...] = (
    "page_view",
    "video_play",
    "video_pause",
    "video_seek",
    "quiz_attempt",
    "quiz_answer",
    "assignment_submit",
    "forum_post",
    "forum_reply",
)

OFFLINE_EVENT_TYPES: tuple[str, ...] = (
    "attendance",
    "library_rfid",
    "grade_record",
)

DLQ_ERROR_TYPES: tuple[str, ...] = (
    "missing_field",
    "schema_mismatch",
    "invalid_timestamp",
    "duplicate_event",
    "null_student_id",
    "invalid_event_type",
    "late_arrival",
)

REQUIRED_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_ts",
    "produced_ts",
    "student_id",
    "event_type",
    "course_id",
    "year_cohort",
    "persona",
    "source",
    "properties",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_ts(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class StudentProfile:
    student_id: str
    year_cohort: int
    persona: str
    device_id: str
    anonymous_id: str


@dataclass(frozen=True)
class StudentEvent:
    event_ts: datetime
    produced_ts: datetime
    student_id: str | None
    event_type: str
    course_id: str
    year_cohort: int
    persona: str
    source: str
    properties: dict[str, Any]
    event_id: str | None = None
    anonymous_id: str | None = None
    device_id: str | None = None
    session_hint_id: str | None = None
    error_type: str | None = None
    is_malformed: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "event_id": self.event_id or str(uuid4()),
            "event_ts": format_ts(self.event_ts),
            "produced_ts": format_ts(self.produced_ts),
            "student_id": self.student_id,
            "anonymous_id": self.anonymous_id,
            "device_id": self.device_id,
            "session_hint_id": self.session_hint_id,
            "event_type": self.event_type,
            "course_id": self.course_id,
            "year_cohort": self.year_cohort,
            "persona": self.persona,
            "source": self.source,
            "properties": self.properties,
            "is_malformed": self.is_malformed,
        }
        if self.error_type:
            payload["error_type"] = self.error_type
        return payload


def validate_event_payload(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    for field in REQUIRED_FIELDS:
        if field not in payload:
            errors.append(f"missing_field:{field}")

    if payload.get("student_id") is None:
        errors.append("null_student_id")

    event_type = payload.get("event_type")
    if event_type not in ONLINE_EVENT_TYPES + OFFLINE_EVENT_TYPES:
        errors.append("invalid_event_type")

    if not isinstance(payload.get("year_cohort"), int):
        errors.append("schema_mismatch:year_cohort")

    if not isinstance(payload.get("properties"), dict):
        errors.append("schema_mismatch:properties")

    for field in ("event_ts", "produced_ts"):
        value = payload.get(field)
        if not isinstance(value, str) or not value.endswith("Z"):
            errors.append(f"invalid_timestamp:{field}")

    return errors
