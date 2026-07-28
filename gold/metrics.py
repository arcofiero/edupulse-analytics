from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bronze.writer import append_jsonl
from silver.transform import load_table

ENGAGEMENT_WEIGHTS = {
    "page_view": 1.0,
    "video_play": 2.0,
    "video_pause": 0.5,
    "video_seek": 0.5,
    "quiz_attempt": 4.0,
    "quiz_answer": 2.0,
    "assignment_submit": 8.0,
    "forum_post": 5.0,
    "forum_reply": 3.0,
}


@dataclass
class GoldResult:
    student_scores: int = 0
    content_rows: int = 0
    adoption_rows: int = 0


class GoldMetricBuilder:
    def __init__(self, silver_root: Path, gold_root: Path) -> None:
        self.silver_root = silver_root
        self.gold_root = gold_root

    def run(self) -> GoldResult:
        student_events = load_table(self.silver_root, "silver_student_events")
        offline_events = load_table(self.silver_root, "silver_offline_events")
        sessions = load_table(self.silver_root, "silver_sessions")

        result = GoldResult()
        for row in self._student_engagement_scores(student_events, offline_events, sessions):
            append_jsonl(self.gold_root / "student_engagement_score" / "data.jsonl", row)
            result.student_scores += 1

        for row in self._course_content_engagement(student_events):
            append_jsonl(self.gold_root / "course_content_engagement" / "data.jsonl", row)
            result.content_rows += 1

        for row in self._department_adoption_weekly(student_events, offline_events):
            append_jsonl(self.gold_root / "department_adoption_weekly" / "data.jsonl", row)
            result.adoption_rows += 1

        return result

    def _student_engagement_scores(
        self,
        student_events: list[dict[str, Any]],
        offline_events: list[dict[str, Any]],
        sessions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        online_score: dict[str, float] = defaultdict(float)
        event_counts: dict[str, int] = defaultdict(int)
        attendance: dict[str, int] = defaultdict(int)
        session_counts: dict[str, int] = defaultdict(int)
        latest_profile: dict[str, dict[str, Any]] = {}

        for event in student_events:
            student_id = event["student_id"]
            online_score[student_id] += ENGAGEMENT_WEIGHTS.get(event["event_type"], 1.0)
            event_counts[student_id] += 1
            latest_profile[student_id] = event

        for event in offline_events:
            student_id = event["student_id"]
            if event["event_type"] == "attendance" and event["properties"].get("present"):
                attendance[student_id] += 1
            latest_profile[student_id] = event

        for session in sessions:
            student_id = session.get("student_id")
            if student_id:
                session_counts[student_id] += 1

        rows = []
        for student_id, profile in sorted(latest_profile.items()):
            score = online_score[student_id] + (attendance[student_id] * 2) + session_counts[student_id]
            risk = self._risk_band(score, event_counts[student_id], profile.get("persona"))
            rows.append(
                {
                    "student_id": student_id,
                    "year_cohort": profile["year_cohort"],
                    "persona": profile["persona"],
                    "engagement_score": round(score, 2),
                    "online_event_count": event_counts[student_id],
                    "attendance_count": attendance[student_id],
                    "session_count": session_counts[student_id],
                    "dropout_risk_band": risk,
                }
            )
        return rows

    def _course_content_engagement(self, student_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for event in student_events:
            key = (event["course_id"], event["event_type"])
            row = grouped.setdefault(
                key,
                {
                    "course_id": event["course_id"],
                    "event_type": event["event_type"],
                    "event_count": 0,
                    "student_ids": set(),
                },
            )
            row["event_count"] += 1
            row["student_ids"].add(event["student_id"])

        rows = []
        for row in grouped.values():
            rows.append(
                {
                    "course_id": row["course_id"],
                    "event_type": row["event_type"],
                    "event_count": row["event_count"],
                    "unique_students": len(row["student_ids"]),
                }
            )
        return sorted(rows, key=lambda row: (row["course_id"], row["event_type"]))

    def _department_adoption_weekly(
        self,
        student_events: list[dict[str, Any]],
        offline_events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        grouped: dict[tuple[int, str], set[str]] = defaultdict(set)
        for event in student_events + offline_events:
            week = event["event_date"][:10]
            grouped[(event["year_cohort"], week)].add(event["student_id"])

        return [
            {
                "year_cohort": year_cohort,
                "week_start_date": week_start_date,
                "active_students": len(student_ids),
            }
            for (year_cohort, week_start_date), student_ids in sorted(grouped.items())
        ]

    def _risk_band(self, score: float, event_count: int, persona: str | None) -> str:
        if persona in {"ghost", "at_risk"} and score < 20:
            return "high"
        if score < 12 or event_count < 4:
            return "high"
        if score < 35:
            return "medium"
        return "low"
