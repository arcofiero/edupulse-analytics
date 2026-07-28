import json
import subprocess
import sys
from datetime import date

from simulator.generator import EventGenerator, SimulationConfig
from simulator.models import DLQ_ERROR_TYPES, validate_event_payload


def test_backfill_generates_valid_events_without_malformed_injection():
    generator = EventGenerator(
        SimulationConfig(
            students=8,
            semester_start_date=date(2025, 9, 1),
            weeks=1,
            malformed_rate=0,
            seed=42,
        )
    )

    events = list(generator.iter_backfill(weeks=1))

    assert events
    assert {event["source"] for event in events} >= {"lms", "campus"}
    assert all(event["is_malformed"] is False for event in events)
    assert all(validate_event_payload(event) == [] for event in events)


def test_malformed_events_include_dlq_error_type():
    generator = EventGenerator(
        SimulationConfig(
            students=4,
            semester_start_date=date(2025, 9, 1),
            weeks=1,
            malformed_rate=1,
            seed=7,
        )
    )

    events = list(generator.iter_backfill(weeks=1))

    assert events
    assert all(event["is_malformed"] is True for event in events)
    assert {event["error_type"] for event in events}.issubset(set(DLQ_ERROR_TYPES))


def test_live_mode_can_be_limited():
    generator = EventGenerator(
        SimulationConfig(
            students=3,
            semester_start_date=date(2025, 9, 1),
            malformed_rate=0,
            seed=11,
        )
    )

    live_events = generator.iter_live()
    events = [next(live_events) for _ in range(3)]

    assert len(events) == 3
    assert all(event["source"] == "lms" for event in events)
    assert all(validate_event_payload(event) == [] for event in events)


def test_cli_emits_json_lines_for_backfill():
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "simulator.main",
            "--mode",
            "backfill",
            "--weeks",
            "1",
            "--students",
            "5",
            "--seed",
            "5",
            "--limit",
            "10",
            "--malformed-rate",
            "0",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    lines = [json.loads(line) for line in result.stdout.splitlines()]

    assert len(lines) == 10
    assert all(validate_event_payload(line) == [] for line in lines)
