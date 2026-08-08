from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean
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
    risk_signal_rows: int = 0
    intervention_rows: int = 0
    content_rows: int = 0
    difficulty_rows: int = 0
    adoption_rows: int = 0
    cohort_rows: int = 0


class GoldMetricBuilder:
    def __init__(self, silver_root: Path, gold_root: Path) -> None:
        self.silver_root = silver_root
        self.gold_root = gold_root

    def run(self) -> GoldResult:
        student_events = load_table(self.silver_root, "silver_student_events")
        offline_events = load_table(self.silver_root, "silver_offline_events")
        sessions = load_table(self.silver_root, "silver_sessions")

        student_scores = self._student_engagement_scores(
            student_events,
            offline_events,
            sessions,
        )
        content_engagement = self._course_content_engagement(student_events)
        difficulty = self._content_difficulty_index(student_events)
        adoption = self._department_adoption_weekly(student_events, offline_events)
        cohort_summary = self._cohort_engagement_summary(student_scores)
        risk_signals = self._student_risk_signals(student_scores)
        interventions = self._advisor_intervention_queue(student_scores)

        result = GoldResult()
        result.student_scores = self._write_table(
            "student_engagement_score",
            "data.jsonl",
            student_scores,
        )
        result.risk_signal_rows = self._write_table(
            "student_risk_signals",
            "data.jsonl",
            risk_signals,
        )
        result.intervention_rows = self._write_table(
            "advisor_intervention_queue",
            "data.jsonl",
            interventions,
        )
        result.content_rows = self._write_table(
            "course_content_engagement",
            "data.jsonl",
            content_engagement,
        )
        result.difficulty_rows = self._write_table(
            "content_difficulty_index",
            "data.jsonl",
            difficulty,
        )
        result.adoption_rows = self._write_table(
            "department_adoption_weekly",
            "data.jsonl",
            adoption,
        )
        result.cohort_rows = self._write_table(
            "cohort_engagement_summary",
            "data.jsonl",
            cohort_summary,
        )
        return result

    def _write_table(self, table: str, file_name: str, rows: list[dict[str, Any]]) -> int:
        for row in rows:
            append_jsonl(self.gold_root / table / file_name, row)
        return len(rows)

    def _student_engagement_scores(
        self,
        student_events: list[dict[str, Any]],
        offline_events: list[dict[str, Any]],
        sessions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        online_score: dict[str, float] = defaultdict(float)
        event_counts: dict[str, int] = defaultdict(int)
        event_types: dict[str, Counter] = defaultdict(Counter)
        active_days: dict[str, set[str]] = defaultdict(set)
        night_events: dict[str, int] = defaultdict(int)
        quiz_answers: dict[str, int] = defaultdict(int)
        quiz_correct: dict[str, int] = defaultdict(int)
        late_submissions: dict[str, int] = defaultdict(int)
        attendance_total: dict[str, int] = defaultdict(int)
        attendance_present: dict[str, int] = defaultdict(int)
        library_visits: dict[str, int] = defaultdict(int)
        session_counts: dict[str, int] = defaultdict(int)
        session_duration: dict[str, int] = defaultdict(int)
        latest_profile: dict[str, dict[str, Any]] = {}
        latest_ts: dict[str, str] = {}

        for event in student_events:
            student_id = event["student_id"]
            event_type = event["event_type"]
            properties = event.get("properties", {})
            online_score[student_id] += ENGAGEMENT_WEIGHTS.get(event_type, 1.0)
            event_counts[student_id] += 1
            event_types[student_id][event_type] += 1
            active_days[student_id].add(event["event_date"])
            latest_profile[student_id] = event
            latest_ts[student_id] = max(latest_ts.get(student_id, ""), event["event_ts"])

            hour = self._parse_ts(event["event_ts"]).hour
            if hour >= 22 or hour < 3:
                night_events[student_id] += 1
            if event_type == "quiz_answer":
                quiz_answers[student_id] += 1
                if properties.get("correct"):
                    quiz_correct[student_id] += 1
            if (
                event_type == "assignment_submit"
                and properties.get("minutes_before_deadline", 0) < 0
            ):
                late_submissions[student_id] += 1

        for event in offline_events:
            student_id = event["student_id"]
            latest_profile[student_id] = event
            latest_ts[student_id] = max(latest_ts.get(student_id, ""), event["event_ts"])
            if event["event_type"] == "attendance":
                attendance_total[student_id] += 1
                if event.get("properties", {}).get("present"):
                    attendance_present[student_id] += 1
            elif event["event_type"] == "library_rfid":
                library_visits[student_id] += 1

        for session in sessions:
            student_id = session.get("student_id")
            if student_id:
                session_counts[student_id] += 1
                session_duration[student_id] += session.get("duration_seconds", 0)

        rows = []
        for student_id, profile in sorted(latest_profile.items()):
            attendance_rate = self._rate(
                attendance_present[student_id],
                attendance_total[student_id],
            )
            quiz_accuracy = self._rate(quiz_correct[student_id], quiz_answers[student_id])
            night_activity_ratio = self._rate(night_events[student_id], event_counts[student_id])
            raw_score = (
                online_score[student_id]
                + (attendance_present[student_id] * 2)
                + session_counts[student_id]
                + (library_visits[student_id] * 0.5)
                + (quiz_accuracy * 5 if quiz_answers[student_id] else 0)
            )
            risk_score, risk_reasons = self._risk_score(
                profile=profile,
                event_count=event_counts[student_id],
                active_day_count=len(active_days[student_id]),
                attendance_rate=attendance_rate,
                quiz_accuracy=quiz_accuracy,
                quiz_answer_count=quiz_answers[student_id],
                assignment_count=event_types[student_id]["assignment_submit"],
                session_count=session_counts[student_id],
                late_submission_count=late_submissions[student_id],
            )
            rows.append(
                {
                    "student_id": student_id,
                    "year_cohort": profile["year_cohort"],
                    "persona": profile["persona"],
                    "engagement_score": round(raw_score, 2),
                    "engagement_percentile": 0,
                    "risk_score": risk_score,
                    "dropout_risk_band": self._risk_band(risk_score),
                    "online_event_count": event_counts[student_id],
                    "active_day_count": len(active_days[student_id]),
                    "attendance_count": attendance_present[student_id],
                    "attendance_rate": attendance_rate,
                    "session_count": session_counts[student_id],
                    "avg_session_minutes": round(
                        session_duration[student_id] / max(session_counts[student_id], 1) / 60,
                        2,
                    ),
                    "quiz_answer_count": quiz_answers[student_id],
                    "quiz_accuracy": quiz_accuracy,
                    "assignment_submit_count": event_types[student_id]["assignment_submit"],
                    "late_submission_count": late_submissions[student_id],
                    "forum_event_count": (
                        event_types[student_id]["forum_post"]
                        + event_types[student_id]["forum_reply"]
                    ),
                    "night_activity_ratio": night_activity_ratio,
                    "library_visit_count": library_visits[student_id],
                    "latest_activity_ts": latest_ts.get(student_id),
                    "risk_reasons": "|".join(risk_reasons),
                    "recommended_action": self._recommended_action(risk_reasons),
                }
            )

        self._assign_percentiles(rows)
        return rows

    def _student_risk_signals(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "student_id": row["student_id"],
                "year_cohort": row["year_cohort"],
                "persona": row["persona"],
                "risk_score": row["risk_score"],
                "dropout_risk_band": row["dropout_risk_band"],
                "engagement_percentile": row["engagement_percentile"],
                "attendance_rate": row["attendance_rate"],
                "quiz_accuracy": row["quiz_accuracy"],
                "active_day_count": row["active_day_count"],
                "late_submission_count": row["late_submission_count"],
                "risk_reasons": row["risk_reasons"],
                "recommended_action": row["recommended_action"],
            }
            for row in rows
        ]

    def _advisor_intervention_queue(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        queue = [
            row
            for row in rows
            if row["dropout_risk_band"] in {"high", "medium"}
        ]
        queue = sorted(
            queue,
            key=lambda row: (
                -row["risk_score"],
                row["engagement_score"],
                row["student_id"],
            ),
        )
        return [
            {
                "queue_rank": index + 1,
                "student_id": row["student_id"],
                "year_cohort": row["year_cohort"],
                "persona": row["persona"],
                "risk_score": row["risk_score"],
                "dropout_risk_band": row["dropout_risk_band"],
                "engagement_score": row["engagement_score"],
                "engagement_percentile": row["engagement_percentile"],
                "latest_activity_ts": row["latest_activity_ts"],
                "risk_reasons": row["risk_reasons"],
                "recommended_action": row["recommended_action"],
            }
            for index, row in enumerate(queue)
        ]

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
                    "week_start_dates": set(),
                },
            )
            row["event_count"] += 1
            row["student_ids"].add(event["student_id"])
            row["week_start_dates"].add(self._week_start(event["event_date"]))

        rows = []
        for row in grouped.values():
            unique_students = len(row["student_ids"])
            rows.append(
                {
                    "course_id": row["course_id"],
                    "event_type": row["event_type"],
                    "event_count": row["event_count"],
                    "unique_students": unique_students,
                    "engagement_per_student": round(
                        row["event_count"] / max(unique_students, 1),
                        2,
                    ),
                    "observed_weeks": len(row["week_start_dates"]),
                }
            )
        return sorted(rows, key=lambda row: (row["course_id"], row["event_type"]))

    def _content_difficulty_index(
        self,
        student_events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        for event in student_events:
            course_id = event["course_id"]
            row = grouped.setdefault(
                course_id,
                {
                    "course_id": course_id,
                    "student_ids": set(),
                    "quiz_attempts": 0,
                    "quiz_answers": 0,
                    "quiz_correct": 0,
                    "video_events": 0,
                    "assignment_submits": 0,
                    "forum_events": 0,
                    "total_events": 0,
                },
            )
            row["student_ids"].add(event["student_id"])
            row["total_events"] += 1
            event_type = event["event_type"]
            properties = event.get("properties", {})
            if event_type == "quiz_attempt":
                row["quiz_attempts"] += 1
            elif event_type == "quiz_answer":
                row["quiz_answers"] += 1
                if properties.get("correct"):
                    row["quiz_correct"] += 1
            elif event_type.startswith("video_"):
                row["video_events"] += 1
            elif event_type == "assignment_submit":
                row["assignment_submits"] += 1
            elif event_type.startswith("forum_"):
                row["forum_events"] += 1

        rows = []
        for row in grouped.values():
            unique_students = len(row["student_ids"])
            quiz_accuracy = self._rate(row["quiz_correct"], row["quiz_answers"])
            attempt_pressure = self._rate(row["quiz_attempts"], unique_students)
            support_seeking = self._rate(row["forum_events"], row["total_events"])
            difficulty_index = min(
                100,
                round(((1 - quiz_accuracy) * 65) + (attempt_pressure * 20) + (support_seeking * 15), 2),
            )
            rows.append(
                {
                    "course_id": row["course_id"],
                    "unique_students": unique_students,
                    "total_events": row["total_events"],
                    "quiz_attempts": row["quiz_attempts"],
                    "quiz_answers": row["quiz_answers"],
                    "quiz_accuracy": quiz_accuracy,
                    "video_events": row["video_events"],
                    "assignment_submits": row["assignment_submits"],
                    "forum_events": row["forum_events"],
                    "difficulty_index": difficulty_index,
                    "content_health_band": self._content_health_band(difficulty_index),
                }
            )
        return sorted(rows, key=lambda row: (-row["difficulty_index"], row["course_id"]))

    def _department_adoption_weekly(
        self,
        student_events: list[dict[str, Any]],
        offline_events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        all_events = student_events + offline_events
        cohort_students: dict[int, set[str]] = defaultdict(set)
        weekly_active: dict[tuple[int, str], set[str]] = defaultdict(set)
        online_counts: dict[tuple[int, str], int] = defaultdict(int)
        offline_counts: dict[tuple[int, str], int] = defaultdict(int)

        for event in all_events:
            cohort = event["year_cohort"]
            week = self._week_start(event["event_date"])
            cohort_students[cohort].add(event["student_id"])
            weekly_active[(cohort, week)].add(event["student_id"])
            if event["source"] == "campus":
                offline_counts[(cohort, week)] += 1
            else:
                online_counts[(cohort, week)] += 1

        rows = []
        for (year_cohort, week_start_date), student_ids in sorted(weekly_active.items()):
            enrolled = len(cohort_students[year_cohort])
            active = len(student_ids)
            rows.append(
                {
                    "year_cohort": year_cohort,
                    "week_start_date": week_start_date,
                    "active_students": active,
                    "observed_students": enrolled,
                    "adoption_rate": self._rate(active, enrolled),
                    "online_event_count": online_counts[(year_cohort, week_start_date)],
                    "offline_event_count": offline_counts[(year_cohort, week_start_date)],
                }
            )
        return rows

    def _cohort_engagement_summary(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[(row["year_cohort"], row["persona"])].append(row)

        summaries = []
        for (year_cohort, persona), cohort_rows in sorted(grouped.items()):
            high_risk_count = sum(
                1 for row in cohort_rows if row["dropout_risk_band"] == "high"
            )
            summaries.append(
                {
                    "year_cohort": year_cohort,
                    "persona": persona,
                    "student_count": len(cohort_rows),
                    "avg_engagement_score": self._avg(cohort_rows, "engagement_score"),
                    "avg_risk_score": self._avg(cohort_rows, "risk_score"),
                    "high_risk_students": high_risk_count,
                    "high_risk_rate": self._rate(high_risk_count, len(cohort_rows)),
                    "avg_attendance_rate": self._avg(cohort_rows, "attendance_rate"),
                    "avg_quiz_accuracy": self._avg(cohort_rows, "quiz_accuracy"),
                    "avg_active_days": self._avg(cohort_rows, "active_day_count"),
                }
            )
        return summaries

    def _risk_score(
        self,
        profile: dict[str, Any],
        event_count: int,
        active_day_count: int,
        attendance_rate: float,
        quiz_accuracy: float,
        quiz_answer_count: int,
        assignment_count: int,
        session_count: int,
        late_submission_count: int,
    ) -> tuple[int, list[str]]:
        score = 0
        reasons: list[str] = []

        if event_count < 4:
            score += 25
            reasons.append("low_lms_activity")
        if active_day_count <= 1:
            score += 15
            reasons.append("low_active_days")
        if attendance_rate < 0.7:
            score += 20
            reasons.append("attendance_gap")
        if quiz_answer_count and quiz_accuracy < 0.55:
            score += 15
            reasons.append("low_quiz_accuracy")
        if assignment_count == 0:
            score += 10
            reasons.append("missing_assignment_signal")
        if session_count == 0:
            score += 10
            reasons.append("no_sessions")
        if late_submission_count:
            score += min(10, late_submission_count * 5)
            reasons.append("late_submissions")
        if profile.get("persona") in {"ghost", "at_risk"}:
            score += 15
            reasons.append("at_risk_behavior_pattern")

        return min(score, 100), reasons or ["healthy_engagement_pattern"]

    def _risk_band(self, risk_score: int) -> str:
        if risk_score >= 55:
            return "high"
        if risk_score >= 30:
            return "medium"
        return "low"

    def _recommended_action(self, reasons: list[str]) -> str:
        if "attendance_gap" in reasons:
            return "Advisor outreach: attendance check-in"
        if "low_quiz_accuracy" in reasons:
            return "Academic support: quiz remediation"
        if "low_lms_activity" in reasons or "low_active_days" in reasons:
            return "Advisor outreach: engagement nudge"
        if "late_submissions" in reasons:
            return "Faculty support: deadline planning"
        return "Monitor"

    def _assign_percentiles(self, rows: list[dict[str, Any]]) -> None:
        by_cohort: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_cohort[row["year_cohort"]].append(row)

        for cohort_rows in by_cohort.values():
            sorted_rows = sorted(cohort_rows, key=lambda row: row["engagement_score"])
            denominator = max(len(sorted_rows) - 1, 1)
            for index, row in enumerate(sorted_rows):
                row["engagement_percentile"] = round((index / denominator) * 100, 2)

    def _content_health_band(self, difficulty_index: float) -> str:
        if difficulty_index >= 70:
            return "needs_review"
        if difficulty_index >= 45:
            return "watch"
        return "healthy"

    def _rate(self, numerator: float, denominator: float) -> float:
        if denominator == 0:
            return 0.0
        return round(numerator / denominator, 4)

    def _avg(self, rows: list[dict[str, Any]], field: str) -> float:
        if not rows:
            return 0.0
        return round(mean(float(row.get(field, 0)) for row in rows), 4)

    def _week_start(self, event_date: str) -> str:
        parsed = datetime.fromisoformat(event_date)
        return (parsed - timedelta(days=parsed.weekday())).date().isoformat()

    def _parse_ts(self, value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
