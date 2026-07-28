from argparse import Namespace
from datetime import date

from bronze.writer import BronzeWriter
from flink.session_stitcher import SessionStitcher
from gold.metrics import GoldMetricBuilder
from pipeline.local import run_pipeline
from schemas.contracts import load_schema, validate_student_event
from silver.transform import SilverTransformer, load_table
from simulator.generator import EventGenerator, SimulationConfig


def test_schemas_load_and_validate_generated_event():
    schema = load_schema("student_event.avsc")
    generator = EventGenerator(
        SimulationConfig(
            students=2,
            semester_start_date=date(2025, 9, 1),
            malformed_rate=0,
            seed=101,
        )
    )

    event = next(generator.iter_backfill(weeks=1))

    assert schema["name"] == "StudentEvent"
    assert validate_student_event(event) == []


def test_bronze_writer_routes_valid_and_invalid_events(tmp_path):
    generator = EventGenerator(
        SimulationConfig(
            students=3,
            semester_start_date=date(2025, 9, 1),
            malformed_rate=0,
            seed=202,
        )
    )
    valid_event = next(generator.iter_backfill(weeks=1))
    invalid_event = dict(valid_event)
    invalid_event["event_type"] = "not_real"

    result = BronzeWriter(tmp_path).write_events([valid_event, invalid_event])

    assert result.accepted == 1
    assert result.dlq == 1
    assert load_table(tmp_path, "bronze_student_events")
    assert load_table(tmp_path, "bronze_dlq_audit")


def test_session_stitcher_splits_after_timeout():
    events = [
        {
            "student_key": "stu_1",
            "student_id": "stu_1",
            "event_ts": "2025-09-01T10:00:00Z",
            "course_id": "CS101",
            "year_cohort": 1,
            "persona": "engaged",
        },
        {
            "student_key": "stu_1",
            "student_id": "stu_1",
            "event_ts": "2025-09-01T10:20:00Z",
            "course_id": "CS101",
            "year_cohort": 1,
            "persona": "engaged",
        },
        {
            "student_key": "stu_1",
            "student_id": "stu_1",
            "event_ts": "2025-09-01T11:05:00Z",
            "course_id": "CS101",
            "year_cohort": 1,
            "persona": "engaged",
        },
    ]

    sessions = SessionStitcher().stitch(events)

    assert len(sessions) == 2
    assert sessions[0]["event_count"] == 2
    assert sessions[1]["event_count"] == 1


def test_local_pipeline_writes_bronze_silver_and_gold(tmp_path):
    summary = run_pipeline(
        Namespace(
            data_dir=tmp_path,
            students=12,
            weeks=1,
            seed=303,
            limit=80,
            malformed_rate=0.1,
            clean=True,
        )
    )

    assert summary["bronze"]["accepted"] > 0
    assert summary["bronze"]["dlq"] > 0
    assert summary["silver"]["student_events"] > 0
    assert summary["silver"]["sessions"] > 0
    assert summary["gold"]["student_scores"] > 0
    assert load_table(tmp_path / "gold", "student_engagement_score")


def test_silver_and_gold_can_run_from_bronze_outputs(tmp_path):
    generator = EventGenerator(
        SimulationConfig(
            students=8,
            semester_start_date=date(2025, 9, 1),
            malformed_rate=0,
            seed=404,
        )
    )
    BronzeWriter(tmp_path / "bronze").write_events(generator.iter_backfill(weeks=1))

    silver_result = SilverTransformer(tmp_path / "bronze", tmp_path / "silver").run()
    gold_result = GoldMetricBuilder(tmp_path / "silver", tmp_path / "gold").run()

    assert silver_result.student_events > 0
    assert silver_result.offline_events > 0
    assert gold_result.student_scores > 0
    assert gold_result.content_rows > 0
