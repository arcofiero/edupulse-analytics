from __future__ import annotations

import argparse
from pathlib import Path

from dashboards.datasets import DashboardDatasetBuilder


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare local dashboard datasets.")
    parser.add_argument("--data-dir", type=Path, default=Path(".local/lakehouse"))
    parser.add_argument("--output-dir", type=Path, default=Path(".local/superset"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = DashboardDatasetBuilder(
        gold_root=args.data_dir / "gold",
        output_dir=args.output_dir,
    ).build()

    for dataset in result.datasets:
        print(
            f"{dataset.dataset_name}: rows={dataset.row_count}, "
            f"file={args.output_dir / dataset.file_name}"
        )
    print(f"sqlite: file={args.output_dir / 'edupulse_dashboards.db'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
