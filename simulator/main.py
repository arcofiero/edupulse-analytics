from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Iterable

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def parse_args() -> argparse.Namespace:
    from config import settings

    parser = argparse.ArgumentParser(description="Generate EduPulse LMS activity events.")
    parser.add_argument("--mode", choices=("live", "backfill"), default=settings.SIMULATOR_MODE)
    parser.add_argument("--weeks", type=int, default=16)
    parser.add_argument("--students", type=int, default=settings.NUM_STUDENTS)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--malformed-rate", type=float, default=0.05)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def build_config(args: argparse.Namespace):
    from config import settings
    from simulator.generator import SimulationConfig

    return SimulationConfig(
        students=args.students,
        academic_year=settings.ACADEMIC_YEAR,
        semester_start_date=date.fromisoformat(settings.SEMESTER_START_DATE),
        weeks=args.weeks,
        malformed_rate=args.malformed_rate,
        seed=args.seed,
    )


def limited(events: Iterable[dict], limit: int | None) -> Iterable[dict]:
    for index, event in enumerate(events):
        if limit is not None and index >= limit:
            break
        yield event


def emit_jsonl(
    events: Iterable[dict],
    output: Path | None,
    sleep_seconds: float | None = None,
) -> int:
    count = 0
    destination = output.open("w", encoding="utf-8") if output else sys.stdout
    try:
        for event in events:
            destination.write(json.dumps(event, sort_keys=True) + "\n")
            destination.flush()
            count += 1
            if sleep_seconds:
                time.sleep(sleep_seconds)
    finally:
        if output:
            destination.close()
    return count


def main() -> int:
    from simulator.generator import EventGenerator

    args = parse_args()
    generator = EventGenerator(build_config(args))

    try:
        if args.mode == "backfill":
            events = limited(generator.iter_backfill(args.weeks), args.limit)
            emit_jsonl(events, args.output)
        else:
            events = limited(generator.iter_live(), args.limit)
            emit_jsonl(events, args.output, args.sleep_seconds)
    except KeyboardInterrupt:
        return 130

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
