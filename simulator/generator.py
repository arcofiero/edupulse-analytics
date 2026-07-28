from __future__ import annotations

import copy
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterator

from simulator.models import (
    DLQ_ERROR_TYPES,
    ONLINE_EVENT_TYPES,
    PERSONA_DISTRIBUTION,
    StudentEvent,
    StudentProfile,
    utc_now,
)


@dataclass(frozen=True)
class SimulationConfig:
    students: int = 500
    academic_year: int = 2025
    semester_start_date: date = date(2025, 9, 1)
    weeks: int = 16
    malformed_rate: float = 0.05
    seed: int | None = None


@dataclass(frozen=True)
class PersonaBehavior:
    daily_activity_chance: float
    min_events: int
    max_events: int
    forum_chance: float
    assignment_chance: float
    night_shift: bool = False
    decline_after_week: int | None = None
    ghost_after_week: int | None = None


PERSONA_BEHAVIOR: dict[str, PersonaBehavior] = {
    "engaged": PersonaBehavior(0.92, 5, 14, 0.28, 0.20),
    "passive": PersonaBehavior(0.58, 2, 7, 0.08, 0.10),
    "at_risk": PersonaBehavior(0.46, 1, 5, 0.03, 0.06, decline_after_week=4),
    "night_owl": PersonaBehavior(0.84, 4, 11, 0.18, 0.16, night_shift=True),
    "ghost": PersonaBehavior(0.35, 1, 3, 0.01, 0.02, ghost_after_week=2),
}

COURSES_BY_COHORT: dict[int, tuple[str, ...]] = {
    1: ("CS101", "MATH101", "WRIT101"),
    2: ("CS201", "STAT210", "HCI220"),
    3: ("CS301", "DATA330", "ML350"),
    4: ("CAP401", "ETH410", "OPS420"),
}


class EventGenerator:
    def __init__(self, config: SimulationConfig) -> None:
        if config.students < 1:
            raise ValueError("students must be greater than zero")
        if config.weeks < 1:
            raise ValueError("weeks must be greater than zero")
        if not 0 <= config.malformed_rate <= 1:
            raise ValueError("malformed_rate must be between 0 and 1")

        self.config = config
        self.rng = random.Random(config.seed)
        self.students = self._build_students()

    def iter_backfill(self, weeks: int | None = None) -> Iterator[dict[str, Any]]:
        week_count = weeks or self.config.weeks
        for week_index in range(week_count):
            for day_offset in range(7):
                event_date = self.config.semester_start_date + timedelta(
                    weeks=week_index,
                    days=day_offset,
                )
                for student in self.students:
                    yield from self._events_for_student_day(student, event_date, week_index)

    def iter_live(self) -> Iterator[dict[str, Any]]:
        while True:
            student = self.rng.choice(self.students)
            event_time = utc_now()
            course_id = self._course_for(student)
            event_type = self._weighted_online_event_type(student.persona)
            event = self._online_event(student, event_time, event_type, course_id)
            yield self._maybe_malformed(event.to_dict())

    def _build_students(self) -> list[StudentProfile]:
        students: list[StudentProfile] = []
        for index in range(self.config.students):
            persona = self._choose_persona()
            year_cohort = self.rng.randint(1, 4)
            students.append(
                StudentProfile(
                    student_id=f"stu_{self.config.academic_year}_{index + 1:05d}",
                    year_cohort=year_cohort,
                    persona=persona,
                    device_id=self._token("dev"),
                    anonymous_id=self._token("anon"),
                )
            )
        return students

    def _choose_persona(self) -> str:
        draw = self.rng.random()
        cumulative = 0.0
        for persona, share in PERSONA_DISTRIBUTION:
            cumulative += share
            if draw <= cumulative:
                return persona
        return PERSONA_DISTRIBUTION[-1][0]

    def _events_for_student_day(
        self,
        student: StudentProfile,
        event_date: date,
        week_index: int,
    ) -> Iterator[dict[str, Any]]:
        behavior = PERSONA_BEHAVIOR[student.persona]
        activity_chance = self._adjusted_activity_chance(behavior, week_index)
        if self.rng.random() > activity_chance:
            return

        course_id = self._course_for(student)
        event_count = self._daily_event_count(behavior, week_index)
        start_hour, end_hour = (22, 27) if behavior.night_shift else (8, 22)
        session_id = self._token("ses")

        for _ in range(event_count):
            event_time = self._random_datetime(event_date, start_hour, end_hour)
            event_type = self._weighted_online_event_type(student.persona)
            event = self._online_event(student, event_time, event_type, course_id, session_id)
            yield self._maybe_malformed(event.to_dict())

        yield from self._offline_events(student, event_date, week_index, course_id)

    def _adjusted_activity_chance(
        self,
        behavior: PersonaBehavior,
        week_index: int,
    ) -> float:
        chance = behavior.daily_activity_chance * self._calendar_multiplier(week_index)
        if behavior.decline_after_week is not None and week_index >= behavior.decline_after_week:
            chance *= max(0.22, 1 - ((week_index - behavior.decline_after_week + 1) * 0.10))
        if behavior.ghost_after_week is not None and week_index >= behavior.ghost_after_week:
            chance *= 0.12
        return min(chance, 0.98)

    def _daily_event_count(self, behavior: PersonaBehavior, week_index: int) -> int:
        event_count = self.rng.randint(behavior.min_events, behavior.max_events)
        if week_index in (0, 6, 7, 13, 14):
            event_count += self.rng.randint(1, 4)
        return event_count

    def _calendar_multiplier(self, week_index: int) -> float:
        if week_index == 0:
            return 1.35
        if week_index in (6, 7):
            return 1.25
        if week_index in (13, 14):
            return 1.45
        if week_index in (4, 5, 9, 10):
            return 0.82
        return 1.0

    def _weighted_online_event_type(self, persona: str) -> str:
        behavior = PERSONA_BEHAVIOR[persona]
        event_types = list(ONLINE_EVENT_TYPES)
        weights = [18, 14, 8, 4, 8, 12, 6, 3, 2]
        weights[6] += int(behavior.assignment_chance * 50)
        weights[7] += int(behavior.forum_chance * 60)
        weights[8] += int(behavior.forum_chance * 40)
        return self.rng.choices(event_types, weights=weights, k=1)[0]

    def _online_event(
        self,
        student: StudentProfile,
        event_time: datetime,
        event_type: str,
        course_id: str,
        session_id: str | None = None,
    ) -> StudentEvent:
        return StudentEvent(
            event_id=self._token("evt"),
            event_ts=event_time,
            produced_ts=event_time + timedelta(seconds=self.rng.randint(1, 90)),
            student_id=student.student_id,
            anonymous_id=student.anonymous_id,
            device_id=student.device_id,
            session_hint_id=session_id or self._token("ses"),
            event_type=event_type,
            course_id=course_id,
            year_cohort=student.year_cohort,
            persona=student.persona,
            source="lms",
            properties=self._properties_for(event_type),
        )

    def _offline_events(
        self,
        student: StudentProfile,
        event_date: date,
        week_index: int,
        course_id: str,
    ) -> Iterator[dict[str, Any]]:
        attendance_time = self._random_datetime(event_date, 9, 17)
        if self.rng.random() < 0.72:
            delay_hours = self.rng.randint(0, 8)
            event = StudentEvent(
                event_id=self._token("evt"),
                event_ts=attendance_time,
                produced_ts=attendance_time + timedelta(hours=delay_hours),
                student_id=student.student_id,
                event_type="attendance",
                course_id=course_id,
                year_cohort=student.year_cohort,
                persona=student.persona,
                source="campus",
                properties={
                    "present": self.rng.random() > 0.12,
                    "delay_hours": delay_hours,
                },
            )
            yield self._maybe_malformed(event.to_dict())

        if self.rng.random() < 0.18:
            library_time = self._random_datetime(event_date, 8, 23)
            delay_hours = self.rng.randint(0, 36)
            event = StudentEvent(
                event_id=self._token("evt"),
                event_ts=library_time,
                produced_ts=library_time + timedelta(hours=delay_hours),
                student_id=student.student_id,
                event_type="library_rfid",
                course_id=course_id,
                year_cohort=student.year_cohort,
                persona=student.persona,
                source="campus",
                properties={
                    "zone": self.rng.choice(("stacks", "study", "lab")),
                    "delay_hours": delay_hours,
                },
            )
            yield self._maybe_malformed(event.to_dict())

        if event_date.weekday() == 4 and week_index > 0 and self.rng.random() < 0.20:
            grade_time = self._random_datetime(event_date, 12, 18)
            event = StudentEvent(
                event_id=self._token("evt"),
                event_ts=grade_time,
                produced_ts=grade_time + timedelta(hours=self.rng.randint(4, 24)),
                student_id=student.student_id,
                event_type="grade_record",
                course_id=course_id,
                year_cohort=student.year_cohort,
                persona=student.persona,
                source="campus",
                properties={
                    "score": round(self.rng.uniform(45, 98), 1),
                    "assessment": "weekly_quiz",
                },
            )
            yield self._maybe_malformed(event.to_dict())

    def _properties_for(self, event_type: str) -> dict[str, Any]:
        if event_type == "page_view":
            return {"page": self.rng.choice(("home", "module", "assignment", "grades"))}
        if event_type.startswith("video_"):
            return {
                "video_id": f"vid_{self.rng.randint(1, 80):03d}",
                "position_seconds": self.rng.randint(0, 3600),
                "playback_rate": self.rng.choice((1.0, 1.25, 1.5, 2.0)),
            }
        if event_type.startswith("quiz_"):
            return {
                "quiz_id": f"quiz_{self.rng.randint(1, 24):03d}",
                "attempt": self.rng.randint(1, 3),
                "correct": self.rng.random() > 0.34,
            }
        if event_type == "assignment_submit":
            return {
                "assignment_id": f"asg_{self.rng.randint(1, 16):03d}",
                "minutes_before_deadline": self.rng.randint(-180, 10080),
            }
        if event_type.startswith("forum_"):
            return {
                "thread_id": f"thr_{self.rng.randint(1, 50):03d}",
                "word_count": self.rng.randint(12, 280),
            }
        return {}

    def _maybe_malformed(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.rng.random() >= self.config.malformed_rate:
            return payload

        malformed = copy.deepcopy(payload)
        error_type = self.rng.choice(DLQ_ERROR_TYPES)
        malformed["is_malformed"] = True
        malformed["error_type"] = error_type

        if error_type == "missing_field":
            malformed.pop(self.rng.choice(("event_id", "course_id", "properties")), None)
        elif error_type == "schema_mismatch":
            malformed["year_cohort"] = str(malformed["year_cohort"])
        elif error_type == "invalid_timestamp":
            malformed["event_ts"] = "not-a-timestamp"
        elif error_type == "null_student_id":
            malformed["student_id"] = None
        elif error_type == "invalid_event_type":
            malformed["event_type"] = "unknown_event"
        elif error_type == "late_arrival":
            malformed["produced_ts"] = self._late_arrival_timestamp(malformed["event_ts"])

        return malformed

    def _course_for(self, student: StudentProfile) -> str:
        return self.rng.choice(COURSES_BY_COHORT[student.year_cohort])

    def _token(self, prefix: str) -> str:
        return f"{prefix}_{self.rng.getrandbits(48):012x}"

    def _random_datetime(self, value: date, start_hour: int, end_hour: int) -> datetime:
        hour = self.rng.randint(start_hour, end_hour - 1)
        day = value
        if hour >= 24:
            hour -= 24
            day = value + timedelta(days=1)
        return datetime.combine(
            day,
            time(hour, self.rng.randint(0, 59), self.rng.randint(0, 59)),
            tzinfo=timezone.utc,
        )

    def _late_arrival_timestamp(self, event_ts: str) -> str:
        parsed = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
        return (parsed + timedelta(hours=37)).isoformat().replace("+00:00", "Z")

    def _current_week_index(self) -> int:
        today = utc_now().date()
        delta_days = max(0, (today - self.config.semester_start_date).days)
        return min(delta_days // 7, self.config.weeks - 1)
