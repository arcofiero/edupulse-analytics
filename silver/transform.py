from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bronze.writer import append_jsonl
from flink.session_stitcher import SessionStitcher


@dataclass
class SilverResult:
    student_events: int = 0
    offline_events: int = 0
    sessions: int = 0


def load_table(root_path: Path, table: str) -> list[dict[str, Any]]:
    table_path = root_path / table
    events: list[dict[str, Any]] = []
    for path in sorted(table_path.glob("**/*.jsonl")):
        with path.open(encoding="utf-8") as input_file:
            events.extend(json.loads(line) for line in input_file if line.strip())
    return events


class SilverTransformer:
    def __init__(self, bronze_root: Path, silver_root: Path) -> None:
        self.bronze_root = bronze_root
        self.silver_root = silver_root

    def run(self) -> SilverResult:
        online_events = load_table(self.bronze_root, "bronze_student_events")
        offline_events = load_table(self.bronze_root, "bronze_offline_events")
        result = SilverResult()

        normalized_online = [self._normalize_event(event) for event in online_events]
        normalized_offline = [self._normalize_event(event) for event in offline_events]

        for event in normalized_online:
            append_jsonl(self.silver_root / "silver_student_events" / "events.jsonl", event)
            result.student_events += 1

        for event in normalized_offline:
            append_jsonl(self.silver_root / "silver_offline_events" / "events.jsonl", event)
            result.offline_events += 1

        for session in SessionStitcher().stitch(normalized_online):
            append_jsonl(self.silver_root / "silver_sessions" / "sessions.jsonl", session)
            result.sessions += 1

        return result

    def _normalize_event(self, event: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(event)
        normalized["event_date"] = event["event_ts"][:10]
        normalized["student_key"] = (
            event.get("student_id")
            or event.get("anonymous_id")
            or event.get("device_id")
            or "unknown"
        )
        normalized["properties_json"] = json.dumps(event.get("properties", {}), sort_keys=True)
        return normalized
