from __future__ import annotations

import csv
import json
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
        content_engagement = load_table(self.gold_root, "course_content_engagement")
        adoption = load_table(self.gold_root, "department_adoption_weekly")

        datasets = [
            self._write_advisor_dataset(student_scores),
            self._write_faculty_dataset(content_engagement),
            self._write_admin_dataset(adoption),
        ]
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
                "year_cohort": row["year_cohort"],
                "persona": row["persona"],
                "engagement_score": row["engagement_score"],
                "online_event_count": row["online_event_count"],
                "attendance_count": row["attendance_count"],
                "session_count": row["session_count"],
                "dropout_risk_band": row["dropout_risk_band"],
                "needs_advisor_review": row["dropout_risk_band"] in {"high", "medium"},
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

    def _write_admin_dataset(self, rows: list[dict[str, Any]]) -> DashboardDataset:
        prepared = [
            {
                "year_cohort": row["year_cohort"],
                "week_start_date": row["week_start_date"],
                "active_students": row["active_students"],
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
            "dashboards": [
                {
                    "name": "Advisor Risk Monitor",
                    "refresh": "< 5 min",
                    "dataset": "advisor_at_risk_students",
                },
                {
                    "name": "Faculty Content Engagement",
                    "refresh": "Daily 6am",
                    "dataset": "faculty_content_engagement",
                },
                {
                    "name": "Department Adoption Weekly",
                    "refresh": "Monday 7am",
                    "dataset": "admin_adoption_weekly",
                },
            ],
            "datasets": [asdict(dataset) for dataset in datasets],
        }
        with (self.output_dir / "manifest.json").open("w", encoding="utf-8") as output_file:
            json.dump(manifest, output_file, indent=2, sort_keys=True)
            output_file.write("\n")

    def _risk_sort(self, risk_band: str | None) -> int:
        ordering = {"high": 0, "medium": 1, "low": 2}
        return ordering.get(risk_band or "", 3)
