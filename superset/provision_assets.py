from __future__ import annotations

import json
import os

from superset.app import create_app


DATABASE_NAME = "EduPulse Local Analytics"
DATABASE_URI = os.getenv(
    "EDUPULSE_ANALYTICS_URI",
    "sqlite:////app/edupulse_superset_data/edupulse_dashboards.db",
)

DATASETS = {
    "advisor_at_risk_students": {
        "dashboard": "Advisor Risk Monitor",
        "chart": "Advisor At-risk Students",
        "columns": [
            "student_id",
            "queue_rank",
            "year_cohort",
            "persona",
            "risk_score",
            "engagement_score",
            "engagement_percentile",
            "dropout_risk_band",
            "needs_advisor_review",
            "risk_reasons",
            "recommended_action",
        ],
    },
    "advisor_engagement_signals": {
        "dashboard": "Advisor Risk Monitor",
        "chart": "Advisor Engagement Signals",
        "columns": [
            "student_id",
            "year_cohort",
            "persona",
            "risk_score",
            "engagement_percentile",
            "active_day_count",
            "attendance_rate",
            "quiz_accuracy",
            "late_submission_count",
            "night_activity_ratio",
            "recommended_action",
        ],
    },
    "faculty_content_engagement": {
        "dashboard": "Faculty Content Engagement",
        "chart": "Faculty Content Engagement",
        "columns": [
            "course_id",
            "event_type",
            "event_count",
            "unique_students",
            "engagement_per_student",
        ],
    },
    "faculty_content_difficulty": {
        "dashboard": "Faculty Content Engagement",
        "chart": "Faculty Content Difficulty",
        "columns": [
            "course_id",
            "content_health_band",
            "difficulty_index",
            "quiz_accuracy",
            "quiz_attempts",
            "quiz_answers",
            "video_events",
            "assignment_submits",
            "forum_events",
            "unique_students",
        ],
    },
    "admin_adoption_weekly": {
        "dashboard": "Department Adoption Weekly",
        "chart": "Department Adoption Weekly",
        "columns": [
            "week_start_date",
            "year_cohort",
            "active_students",
            "observed_students",
            "adoption_rate",
            "online_event_count",
            "offline_event_count",
        ],
    },
    "admin_cohort_engagement_summary": {
        "dashboard": "Department Adoption Weekly",
        "chart": "Cohort Engagement Summary",
        "columns": [
            "year_cohort",
            "persona",
            "student_count",
            "avg_engagement_score",
            "avg_risk_score",
            "high_risk_students",
            "high_risk_rate",
            "avg_attendance_rate",
            "avg_quiz_accuracy",
            "avg_active_days",
        ],
    },
}


def ensure_database(db, Database):
    database = db.session.query(Database).filter_by(database_name=DATABASE_NAME).one_or_none()
    if database is None:
        database = Database(database_name=DATABASE_NAME)
        db.session.add(database)
    database.sqlalchemy_uri = DATABASE_URI
    db.session.flush()
    return database


def ensure_dataset(db, SqlaTable, database, table_name: str):
    dataset = (
        db.session.query(SqlaTable)
        .filter_by(database_id=database.id, table_name=table_name)
        .one_or_none()
    )
    if dataset is None:
        dataset = SqlaTable(table_name=table_name, database=database)
        db.session.add(dataset)
        db.session.flush()

    dataset.fetch_metadata()
    db.session.flush()
    return dataset


def ensure_chart(db, Slice, dataset, chart_name: str, columns: list[str]):
    chart = db.session.query(Slice).filter_by(slice_name=chart_name).one_or_none()
    params = {
        "datasource": f"{dataset.id}__table",
        "viz_type": "table",
        "all_columns": columns,
        "row_limit": 1000,
    }
    if chart is None:
        chart = Slice(slice_name=chart_name)
        db.session.add(chart)

    chart.datasource_id = dataset.id
    chart.datasource_type = "table"
    chart.datasource_name = dataset.table_name
    chart.viz_type = "table"
    chart.params = json.dumps(params)
    db.session.flush()
    return chart


def ensure_dashboard(db, Dashboard, dashboard_title: str, charts: list) -> None:
    dashboard = (
        db.session.query(Dashboard)
        .filter_by(dashboard_title=dashboard_title)
        .one_or_none()
    )
    chart_nodes = [f"CHART-{chart.id}" for chart in charts]
    position_json = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": chart_nodes},
    }
    for index, chart in enumerate(charts):
        chart_node = f"CHART-{chart.id}"
        position_json[chart_node] = {
            "type": "CHART",
            "id": chart_node,
            "children": [],
            "meta": {"chartId": chart.id, "height": 40, "width": 6 if index else 12},
        }

    if dashboard is None:
        dashboard = Dashboard(dashboard_title=dashboard_title)
        db.session.add(dashboard)

    dashboard.published = True
    dashboard.position_json = json.dumps(position_json)
    dashboard.slices = charts
    db.session.flush()


def provision() -> None:
    from superset import db
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    database = ensure_database(db, Database)
    charts_by_dashboard = {}
    for table_name, spec in DATASETS.items():
        dataset = ensure_dataset(db, SqlaTable, database, table_name)
        chart = ensure_chart(db, Slice, dataset, spec["chart"], spec["columns"])
        charts_by_dashboard.setdefault(spec["dashboard"], []).append(chart)

    for dashboard_title, charts in charts_by_dashboard.items():
        ensure_dashboard(db, Dashboard, dashboard_title, charts)

    db.session.commit()
    print("Provisioned EduPulse Superset database, datasets, charts, and dashboards.")


if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        provision()
