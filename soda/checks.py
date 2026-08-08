from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from silver.transform import load_table


class QualityGateError(RuntimeError):
    def __init__(self, report: "QualityReport") -> None:
        failed = ", ".join(check.name for check in report.failed_checks)
        super().__init__(f"{report.layer} quality gate failed: {failed}")
        self.report = report


@dataclass(frozen=True)
class QualityCheckResult:
    layer: str
    name: str
    passed: bool
    details: str


@dataclass(frozen=True)
class QualityReport:
    layer: str
    checks: list[QualityCheckResult]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failed_checks(self) -> list[QualityCheckResult]:
        return [check for check in self.checks if not check.passed]

    def summary(self) -> dict[str, int | bool]:
        failed = len(self.failed_checks)
        return {
            "passed": self.passed,
            "checks": len(self.checks),
            "failed": failed,
        }


class LocalQualityRunner:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir

    def run_layer(self, layer: str) -> QualityReport:
        checks_by_layer: dict[str, Callable[[], list[QualityCheckResult]]] = {
            "bronze": self._bronze_checks,
            "silver": self._silver_checks,
            "gold": self._gold_checks,
        }
        if layer not in checks_by_layer:
            raise ValueError(f"Unsupported quality layer: {layer}")
        return QualityReport(layer=layer, checks=checks_by_layer[layer]())

    def enforce_layer(self, layer: str) -> QualityReport:
        report = self.run_layer(layer)
        if not report.passed:
            raise QualityGateError(report)
        return report

    def _bronze_checks(self) -> list[QualityCheckResult]:
        root = self.data_dir / "bronze"
        student_events = load_table(root, "bronze_student_events")
        offline_events = load_table(root, "bronze_offline_events")
        accepted_events = student_events + offline_events
        dlq_events = load_table(root, "bronze_dlq_audit")

        return [
            self._result("bronze", "accepted_events_not_empty", bool(accepted_events), f"rows={len(accepted_events)}"),
            self._result("bronze", "student_events_not_empty", bool(student_events), f"rows={len(student_events)}"),
            self._required_fields("bronze", "accepted_required_fields", accepted_events, ("event_id", "event_ts", "student_id", "event_type", "course_id", "year_cohort")),
            self._unique_values("bronze", "accepted_event_id_unique", accepted_events, "event_id"),
            self._result("bronze", "no_malformed_in_accepted", not any(event.get("is_malformed") for event in accepted_events), f"rows={len(accepted_events)}"),
            self._required_fields("bronze", "dlq_contract_fields", dlq_events, ("error_type", "raw_payload")),
        ]

    def _silver_checks(self) -> list[QualityCheckResult]:
        root = self.data_dir / "silver"
        student_events = load_table(root, "silver_student_events")
        offline_events = load_table(root, "silver_offline_events")
        sessions = load_table(root, "silver_sessions")

        return [
            self._result("silver", "student_events_not_empty", bool(student_events), f"rows={len(student_events)}"),
            self._required_fields("silver", "student_event_keys_present", student_events, ("event_id", "student_key", "event_date", "year_cohort")),
            self._unique_values("silver", "student_event_id_unique", student_events, "event_id"),
            self._result("silver", "sessions_not_empty", bool(sessions), f"rows={len(sessions)}"),
            self._required_fields("silver", "session_fields_present", sessions, ("session_id", "student_key", "start_ts", "end_ts", "event_count")),
            self._result("silver", "session_event_counts_positive", all(session.get("event_count", 0) > 0 for session in sessions), f"rows={len(sessions)}"),
            self._result("silver", "offline_events_readable", offline_events is not None, f"rows={len(offline_events)}"),
        ]

    def _gold_checks(self) -> list[QualityCheckResult]:
        root = self.data_dir / "gold"
        scores = load_table(root, "student_engagement_score")
        risk_signals = load_table(root, "student_risk_signals")
        interventions = load_table(root, "advisor_intervention_queue")
        content = load_table(root, "course_content_engagement")
        difficulty = load_table(root, "content_difficulty_index")
        adoption = load_table(root, "department_adoption_weekly")
        cohorts = load_table(root, "cohort_engagement_summary")
        risk_values = {"low", "medium", "high"}
        content_health_values = {"healthy", "watch", "needs_review"}

        return [
            self._result("gold", "student_scores_not_empty", bool(scores), f"rows={len(scores)}"),
            self._required_fields("gold", "student_score_fields_present", scores, ("student_id", "engagement_score", "risk_score", "dropout_risk_band")),
            self._unique_values("gold", "student_score_student_id_unique", scores, "student_id"),
            self._result("gold", "engagement_scores_non_negative", all(row.get("engagement_score", -1) >= 0 for row in scores), f"rows={len(scores)}"),
            self._result("gold", "risk_scores_bounded", all(0 <= row.get("risk_score", -1) <= 100 for row in scores), f"rows={len(scores)}"),
            self._result("gold", "risk_bands_valid", all(row.get("dropout_risk_band") in risk_values for row in scores), f"rows={len(scores)}"),
            self._result("gold", "risk_signals_not_empty", bool(risk_signals), f"rows={len(risk_signals)}"),
            self._required_fields("gold", "risk_signal_fields_present", risk_signals, ("student_id", "risk_score", "risk_reasons", "recommended_action")),
            self._result("gold", "intervention_queue_readable", interventions is not None, f"rows={len(interventions)}"),
            self._required_fields("gold", "intervention_queue_fields_present", interventions, ("queue_rank", "student_id", "recommended_action")),
            self._result("gold", "content_engagement_not_empty", bool(content), f"rows={len(content)}"),
            self._result("gold", "content_event_counts_positive", all(row.get("event_count", 0) > 0 for row in content), f"rows={len(content)}"),
            self._result("gold", "content_difficulty_not_empty", bool(difficulty), f"rows={len(difficulty)}"),
            self._result("gold", "difficulty_index_bounded", all(0 <= row.get("difficulty_index", -1) <= 100 for row in difficulty), f"rows={len(difficulty)}"),
            self._result("gold", "content_health_bands_valid", all(row.get("content_health_band") in content_health_values for row in difficulty), f"rows={len(difficulty)}"),
            self._result("gold", "adoption_not_empty", bool(adoption), f"rows={len(adoption)}"),
            self._result("gold", "adoption_active_students_positive", all(row.get("active_students", 0) > 0 for row in adoption), f"rows={len(adoption)}"),
            self._result("gold", "adoption_rates_bounded", all(0 <= row.get("adoption_rate", -1) <= 1 for row in adoption), f"rows={len(adoption)}"),
            self._result("gold", "cohort_summary_not_empty", bool(cohorts), f"rows={len(cohorts)}"),
            self._required_fields("gold", "cohort_summary_fields_present", cohorts, ("year_cohort", "persona", "avg_engagement_score", "high_risk_rate")),
        ]

    def _required_fields(
        self,
        layer: str,
        name: str,
        rows: list[dict[str, Any]],
        fields: tuple[str, ...],
    ) -> QualityCheckResult:
        missing = [
            field
            for row in rows
            for field in fields
            if field not in row or row[field] in (None, "")
        ]
        return self._result(layer, name, not missing, f"missing={len(missing)}")

    def _unique_values(
        self,
        layer: str,
        name: str,
        rows: list[dict[str, Any]],
        field: str,
    ) -> QualityCheckResult:
        values = [row.get(field) for row in rows if row.get(field) is not None]
        return self._result(layer, name, len(values) == len(set(values)), f"rows={len(rows)}")

    def _result(self, layer: str, name: str, passed: bool, details: str) -> QualityCheckResult:
        return QualityCheckResult(layer=layer, name=name, passed=passed, details=details)
