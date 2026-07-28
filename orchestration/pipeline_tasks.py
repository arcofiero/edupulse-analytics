from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from pipeline.local import run_pipeline
from soda.checks import LocalQualityRunner


def run_local_pipeline_task(
    data_dir: str = ".local/lakehouse",
    students: int = 50,
    weeks: int = 1,
    seed: int = 42,
    limit: int | None = 500,
    malformed_rate: float = 0.05,
    clean: bool = True,
) -> dict[str, dict]:
    return run_pipeline(
        Namespace(
            data_dir=Path(data_dir),
            students=students,
            weeks=weeks,
            seed=seed,
            limit=limit,
            malformed_rate=malformed_rate,
            clean=clean,
            skip_quality_gates=False,
        )
    )


def run_quality_gate_task(layer: str, data_dir: str = ".local/lakehouse") -> dict[str, int | bool]:
    report = LocalQualityRunner(Path(data_dir)).enforce_layer(layer)
    return report.summary()
