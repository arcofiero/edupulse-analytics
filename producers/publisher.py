from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from config import settings


class EventPublisher(Protocol):
    def publish(self, payload: dict[str, Any]) -> None:
        ...

    def flush(self) -> None:
        ...


def topic_for_event(payload: dict[str, Any]) -> str:
    if payload.get("is_malformed"):
        return settings.TOPIC_DLQ
    if payload.get("source") == "campus":
        return settings.TOPIC_OFFLINE_EVENTS
    return settings.TOPIC_STUDENT_EVENTS


@dataclass
class LocalJsonlPublisher:
    output_path: Path

    def __post_init__(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.output_path.open("a", encoding="utf-8")

    def publish(self, payload: dict[str, Any]) -> None:
        envelope = {
            "topic": topic_for_event(payload),
            "key": str(payload.get("year_cohort", "unknown")),
            "payload": payload,
        }
        self._handle.write(json.dumps(envelope, sort_keys=True) + "\n")

    def flush(self) -> None:
        self._handle.flush()
        self._handle.close()


class KafkaEventPublisher:
    def __init__(self) -> None:
        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError("confluent-kafka is required for Kafka publishing") from exc

        self._producer = Producer(settings.get_kafka_producer_config())

    def publish(self, payload: dict[str, Any]) -> None:
        self._producer.produce(
            topic_for_event(payload),
            key=str(payload.get("year_cohort", "unknown")),
            value=json.dumps(payload, sort_keys=True).encode("utf-8"),
        )

    def flush(self) -> None:
        self._producer.flush()
