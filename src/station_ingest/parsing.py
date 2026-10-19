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


def reject_non_json_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON constant: {value}")


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
        original_payload = payload.decode("utf-8")
        decoded: Any = json.loads(
            original_payload, parse_constant=reject_non_json_constant
        )
    except (UnicodeDecodeError, ValueError, RecursionError):
        return ParseFailure("invalid_json")

    if not isinstance(decoded, dict):
        return ParseFailure("payload_not_object")

    try:
        event = StationEvent.model_validate(decoded)
    except ValidationError:
        return ParseFailure("invalid_or_missing_station_id")

    timestamp = (
        received_at_ms if received_at_ms is not None else time.time_ns() // 1_000_000
    )
    return BufferedEvent(
        event=event,
        topic=topic,
        received_at_ms=timestamp,
        original_payload=original_payload,
    )
