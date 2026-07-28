from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

SESSION_TIMEOUT = timedelta(minutes=30)


@dataclass(frozen=True)
class SessionStitcher:
    timeout: timedelta = SESSION_TIMEOUT

    def stitch(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for event in events:
            grouped[event["student_key"]].append(event)

        sessions: list[dict[str, Any]] = []
        for student_key, student_events in grouped.items():
            sorted_events = sorted(student_events, key=lambda event: event["event_ts"])
            current: list[dict[str, Any]] = []
            session_index = 1

            for event in sorted_events:
                if current and self._gap(current[-1], event) > self.timeout:
                    sessions.append(self._session_payload(student_key, session_index, current))
                    session_index += 1
                    current = []
                current.append(event)

            if current:
                sessions.append(self._session_payload(student_key, session_index, current))

        return sessions

    def _gap(self, previous: dict[str, Any], current: dict[str, Any]) -> timedelta:
        return self._parse_ts(current["event_ts"]) - self._parse_ts(previous["event_ts"])

    def _session_payload(
        self,
        student_key: str,
        session_index: int,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        start_ts = events[0]["event_ts"]
        end_ts = events[-1]["event_ts"]
        duration_seconds = max(
            0,
            int((self._parse_ts(end_ts) - self._parse_ts(start_ts)).total_seconds()),
        )
        return {
            "session_id": f"session_{student_key}_{session_index:04d}",
            "student_key": student_key,
            "student_id": events[-1].get("student_id"),
            "year_cohort": events[-1].get("year_cohort"),
            "persona": events[-1].get("persona"),
            "course_ids": sorted({event["course_id"] for event in events}),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "duration_seconds": duration_seconds,
            "event_count": len(events),
        }

    def _parse_ts(self, value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
