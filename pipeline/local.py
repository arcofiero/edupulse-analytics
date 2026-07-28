from __future__ import annotations

import argparse
import shutil
from dataclasses import asdict
from datetime import date
from pathlib import Path

from bronze.writer import BronzeWriter
from gold.metrics import GoldMetricBuilder
from silver.transform import SilverTransformer
from soda.checks import LocalQualityRunner
from simulator.generator import EventGenerator, SimulationConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local EduPulse pipeline.")
    parser.add_argument("--data-dir", type=Path, default=Path(".local/lakehouse"))
    parser.add_argument("--students", type=int, default=50)
    parser.add_argument("--weeks", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--malformed-rate", type=float, default=0.05)
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--skip-quality-gates", action="store_true")
    return parser.parse_args()


def run_pipeline(args: argparse.Namespace) -> dict[str, dict]:
    if args.clean and args.data_dir.exists():
        shutil.rmtree(args.data_dir)

    config = SimulationConfig(
        students=args.students,
        semester_start_date=date(2025, 9, 1),
        weeks=args.weeks,
        seed=args.seed,
        malformed_rate=args.malformed_rate,
    )
    generator = EventGenerator(config)
    events = generator.iter_backfill(weeks=args.weeks)
    if args.limit is not None:
        events = _limited(events, args.limit)

    quality = LocalQualityRunner(args.data_dir)

    bronze_result = BronzeWriter(args.data_dir / "bronze").write_events(events)
    skip_quality_gates = getattr(args, "skip_quality_gates", False)
    bronze_quality = _quality_gate(quality, "bronze", skip_quality_gates)

    silver_result = SilverTransformer(args.data_dir / "bronze", args.data_dir / "silver").run()
    silver_quality = _quality_gate(quality, "silver", skip_quality_gates)

    gold_result = GoldMetricBuilder(args.data_dir / "silver", args.data_dir / "gold").run()
    gold_quality = _quality_gate(quality, "gold", skip_quality_gates)

    return {
        "bronze": asdict(bronze_result),
        "silver": asdict(silver_result),
        "gold": asdict(gold_result),
        "quality": {
            "bronze": bronze_quality.summary(),
            "silver": silver_quality.summary(),
            "gold": gold_quality.summary(),
        },
    }


def _limited(events, limit: int):
    for index, event in enumerate(events):
        if index >= limit:
            break
        yield event


def _quality_gate(quality: LocalQualityRunner, layer: str, skip_gates: bool):
    if skip_gates:
        return quality.run_layer(layer)
    return quality.enforce_layer(layer)


def main() -> int:
    summary = run_pipeline(parse_args())
    for layer, counts in summary.items():
        if layer == "quality":
            for quality_layer, quality_counts in counts.items():
                joined_counts = ", ".join(
                    f"{key}={value}" for key, value in quality_counts.items()
                )
                print(f"quality.{quality_layer}: {joined_counts}")
        else:
            joined_counts = ", ".join(f"{key}={value}" for key, value in counts.items())
            print(f"{layer}: {joined_counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
