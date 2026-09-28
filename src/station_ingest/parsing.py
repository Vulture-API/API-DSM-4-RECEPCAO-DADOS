from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from station_ingest.models import BufferedEvent, StationEvent


@dataclass(frozen=True, slots=True)
class ParseFailure:
    reason: str


def parse_message(
    payload: bytes,
    topic: str,
    max_payload_bytes: int,
    *,
    received_at_ms: int | None = None,
) -> BufferedEvent | ParseFailure:
    if len(payload) > max_payload_bytes:
        return ParseFailure("payload_too_large")

    try:
        decoded: Any = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return ParseFailure("invalid_json")

    if not isinstance(decoded, dict):
        return ParseFailure("payload_not_object")

    try:
        event = StationEvent.model_validate(decoded)
    except ValidationError:
        return ParseFailure("invalid_or_missing_station_id")

    timestamp = received_at_ms if received_at_ms is not None else time.time_ns() // 1_000_000
    return BufferedEvent(event=event, topic=topic, received_at_ms=timestamp)

