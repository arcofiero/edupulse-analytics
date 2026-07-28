from __future__ import annotations

import argparse
from pathlib import Path

from soda.checks import LocalQualityRunner


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local EduPulse quality checks.")
    parser.add_argument("--data-dir", type=Path, default=Path(".local/lakehouse"))
    parser.add_argument("--layer", choices=("all", "bronze", "silver", "gold"), default="all")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runner = LocalQualityRunner(args.data_dir)
    layers = ("bronze", "silver", "gold") if args.layer == "all" else (args.layer,)
    failed = False

    for layer in layers:
        report = runner.run_layer(layer)
        failed = failed or not report.passed
        summary = report.summary()
        print(
            f"{layer}: passed={summary['passed']}, "
            f"checks={summary['checks']}, failed={summary['failed']}"
        )
        for check in report.failed_checks:
            print(f"  - {check.name}: {check.details}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
