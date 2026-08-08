from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from silver.transform import load_table


@dataclass(frozen=True)
class DashboardDataset:
    dashboard: str
    dataset_name: str
    file_name: str
    row_count: int
    description: str


@dataclass(frozen=True)
class DashboardBuildResult:
    output_dir: str
    datasets: list[DashboardDataset]

    def summary(self) -> dict[str, Any]:
        return {
            "output_dir": self.output_dir,
            "datasets": [asdict(dataset) for dataset in self.datasets],
        }


class DashboardDatasetBuilder:
    def __init__(self, gold_root: Path, output_dir: Path) -> None:
        self.gold_root = gold_root
        self.output_dir = output_dir

    def build(self) -> DashboardBuildResult:
        self.output_dir.mkdir(parents=True, exist_ok=True)

        student_scores = load_table(self.gold_root, "student_engagement_score")
        risk_queue = load_table(self.gold_root, "advisor_intervention_queue")
        content_engagement = load_table(self.gold_root, "course_content_engagement")
        content_difficulty = load_table(self.gold_root, "content_difficulty_index")
        adoption = load_table(self.gold_root, "department_adoption_weekly")
        cohort_summary = load_table(self.gold_root, "cohort_engagement_summary")

        datasets = [
            self._write_advisor_dataset(risk_queue or student_scores),
            self._write_advisor_signal_dataset(student_scores),
            self._write_faculty_dataset(content_engagement),
            self._write_faculty_difficulty_dataset(content_difficulty),
            self._write_admin_dataset(adoption),
            self._write_admin_cohort_dataset(cohort_summary),
        ]
        self._write_sqlite_database(datasets)
        self._write_manifest(datasets)
        return DashboardBuildResult(output_dir=str(self.output_dir), datasets=datasets)

    def _write_advisor_dataset(self, rows: list[dict[str, Any]]) -> DashboardDataset:
        ordered_rows = sorted(
            rows,
            key=lambda row: (
                self._risk_sort(row.get("dropout_risk_band")),
                float(row.get("engagement_score", 0)),
                row.get("student_id", ""),
            ),
        )
        prepared = [
            {
                "student_id": row["student_id"],
                "queue_rank": row.get("queue_rank"),
                "year_cohort": row["year_cohort"],
                "persona": row["persona"],
                "risk_score": row["risk_score"],
                "engagement_score": row["engagement_score"],
                "engagement_percentile": row["engagement_percentile"],
                "dropout_risk_band": row["dropout_risk_band"],
                "needs_advisor_review": row["dropout_risk_band"] in {"high", "medium"},
                "risk_reasons": row["risk_reasons"],
                "recommended_action": row["recommended_action"],
                "latest_activity_ts": row.get("latest_activity_ts"),
            }
            for row in ordered_rows
        ]
        return self._write_csv(
            dashboard="Advisor",
            dataset_name="advisor_at_risk_students",
            file_name="advisor_at_risk_students.csv",
            rows=prepared,
            description="At-risk student list for advisor intervention workflow.",
        )

    def _write_advisor_signal_dataset(self, rows: list[dict[str, Any]]) -> DashboardDataset:
        prepared = [
            {
                "student_id": row["student_id"],
                "year_cohort": row["year_cohort"],
                "persona": row["persona"],
                "risk_score": row["risk_score"],
                "engagement_percentile": row["engagement_percentile"],
                "active_day_count": row["active_day_count"],
                "attendance_rate": row["attendance_rate"],
                "quiz_accuracy": row["quiz_accuracy"],
                "late_submission_count": row["late_submission_count"],
                "night_activity_ratio": row["night_activity_ratio"],
                "recommended_action": row["recommended_action"],
            }
            for row in sorted(rows, key=lambda row: (-row["risk_score"], row["student_id"]))
        ]
        return self._write_csv(
            dashboard="Advisor",
            dataset_name="advisor_engagement_signals",
            file_name="advisor_engagement_signals.csv",
            rows=prepared,
            description="Student-level risk features used to explain advisor prioritization.",
        )

    def _write_faculty_dataset(self, rows: list[dict[str, Any]]) -> DashboardDataset:
        prepared = [
            {
                "course_id": row["course_id"],
                "event_type": row["event_type"],
                "event_count": row["event_count"],
                "unique_students": row["unique_students"],
                "engagement_per_student": round(
                    row["event_count"] / max(row["unique_students"], 1),
                    2,
                ),
            }
            for row in sorted(rows, key=lambda row: (row["course_id"], row["event_type"]))
        ]
        return self._write_csv(
            dashboard="Faculty",
            dataset_name="faculty_content_engagement",
            file_name="faculty_content_engagement.csv",
            rows=prepared,
            description="Course content engagement by event type for faculty review.",
        )

    def _write_faculty_difficulty_dataset(
        self,
        rows: list[dict[str, Any]],
    ) -> DashboardDataset:
        prepared = [
            {
                "course_id": row["course_id"],
                "content_health_band": row["content_health_band"],
                "difficulty_index": row["difficulty_index"],
                "quiz_accuracy": row["quiz_accuracy"],
                "quiz_attempts": row["quiz_attempts"],
                "quiz_answers": row["quiz_answers"],
                "video_events": row["video_events"],
                "assignment_submits": row["assignment_submits"],
                "forum_events": row["forum_events"],
                "unique_students": row["unique_students"],
                "total_events": row["total_events"],
            }
            for row in sorted(rows, key=lambda row: (-row["difficulty_index"], row["course_id"]))
        ]
        return self._write_csv(
            dashboard="Faculty",
            dataset_name="faculty_content_difficulty",
            file_name="faculty_content_difficulty.csv",
            rows=prepared,
            description="Course-level difficulty and content health indicators.",
        )

    def _write_admin_dataset(self, rows: list[dict[str, Any]]) -> DashboardDataset:
        prepared = [
            {
                "year_cohort": row["year_cohort"],
                "week_start_date": row["week_start_date"],
                "active_students": row["active_students"],
                "observed_students": row["observed_students"],
                "adoption_rate": row["adoption_rate"],
                "online_event_count": row["online_event_count"],
                "offline_event_count": row["offline_event_count"],
            }
            for row in sorted(rows, key=lambda row: (row["week_start_date"], row["year_cohort"]))
        ]
        return self._write_csv(
            dashboard="Admin",
            dataset_name="admin_adoption_weekly",
            file_name="admin_adoption_weekly.csv",
            rows=prepared,
            description="Weekly platform adoption trend by year cohort.",
        )

    def _write_admin_cohort_dataset(self, rows: list[dict[str, Any]]) -> DashboardDataset:
        prepared = [
            {
                "year_cohort": row["year_cohort"],
                "persona": row["persona"],
                "student_count": row["student_count"],
                "avg_engagement_score": row["avg_engagement_score"],
                "avg_risk_score": row["avg_risk_score"],
                "high_risk_students": row["high_risk_students"],
                "high_risk_rate": row["high_risk_rate"],
                "avg_attendance_rate": row["avg_attendance_rate"],
                "avg_quiz_accuracy": row["avg_quiz_accuracy"],
                "avg_active_days": row["avg_active_days"],
            }
            for row in sorted(rows, key=lambda row: (row["year_cohort"], row["persona"]))
        ]
        return self._write_csv(
            dashboard="Admin",
            dataset_name="admin_cohort_engagement_summary",
            file_name="admin_cohort_engagement_summary.csv",
            rows=prepared,
            description="Cohort and persona-level benchmark metrics for program leaders.",
        )

    def _write_csv(
        self,
        dashboard: str,
        dataset_name: str,
        file_name: str,
        rows: list[dict[str, Any]],
        description: str,
    ) -> DashboardDataset:
        output_path = self.output_dir / file_name
        fieldnames = list(rows[0].keys()) if rows else []
        with output_path.open("w", encoding="utf-8", newline="") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            if fieldnames:
                writer.writeheader()
                writer.writerows(rows)
        return DashboardDataset(
            dashboard=dashboard,
            dataset_name=dataset_name,
            file_name=file_name,
            row_count=len(rows),
            description=description,
        )

    def _write_manifest(self, datasets: list[DashboardDataset]) -> None:
        manifest = {
            "database": {
                "name": "EduPulse Local Analytics",
                "sqlalchemy_uri": "sqlite:////app/edupulse_superset_data/edupulse_dashboards.db",
            },
            "dashboards": [
                {
                    "name": "Advisor Risk Monitor",
                    "refresh": "< 5 min",
                    "datasets": [
                        "advisor_at_risk_students",
                        "advisor_engagement_signals",
                    ],
                },
                {
                    "name": "Faculty Content Engagement",
                    "refresh": "Daily 6am",
                    "datasets": [
                        "faculty_content_engagement",
                        "faculty_content_difficulty",
                    ],
                },
                {
                    "name": "Department Adoption Weekly",
                    "refresh": "Monday 7am",
                    "datasets": [
                        "admin_adoption_weekly",
                        "admin_cohort_engagement_summary",
                    ],
                },
            ],
            "datasets": [asdict(dataset) for dataset in datasets],
        }
        with (self.output_dir / "manifest.json").open("w", encoding="utf-8") as output_file:
            json.dump(manifest, output_file, indent=2, sort_keys=True)
            output_file.write("\n")

    def _write_sqlite_database(self, datasets: list[DashboardDataset]) -> None:
        db_path = self.output_dir / "edupulse_dashboards.db"
        if db_path.exists():
            db_path.unlink()

        with sqlite3.connect(db_path) as connection:
            for dataset in datasets:
                csv_path = self.output_dir / dataset.file_name
                with csv_path.open(encoding="utf-8") as input_file:
                    rows = list(csv.DictReader(input_file))
                if not rows:
                    continue

                columns = list(rows[0].keys())
                quoted_columns = ", ".join(f'"{column}" TEXT' for column in columns)
                placeholders = ", ".join("?" for _ in columns)
                connection.execute(f'DROP TABLE IF EXISTS "{dataset.dataset_name}"')
                connection.execute(f'CREATE TABLE "{dataset.dataset_name}" ({quoted_columns})')
                connection.executemany(
                    f'INSERT INTO "{dataset.dataset_name}" VALUES ({placeholders})',
                    [[row[column] for column in columns] for row in rows],
                )

    def _risk_sort(self, risk_band: str | None) -> int:
        ordering = {"high": 0, "medium": 1, "low": 2}
        return ordering.get(risk_band or "", 3)
