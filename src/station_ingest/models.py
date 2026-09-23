from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StationEvent(BaseModel):
    """Validated station event while preserving sensor-specific fields."""

    model_config = ConfigDict(extra="allow")

    estacao_id: str = Field(min_length=1)
    unix_time: int | None = None

    @field_validator("estacao_id", mode="before")
    @classmethod
    def validate_station_id(cls, value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("estacao_id must be a non-empty string")
        return value.strip()

    @field_validator("unix_time", mode="before")
    @classmethod
    def normalize_unix_time(cls, value: Any) -> int | None:
        if type(value) is int and value >= 0:
            return value
        return None


@dataclass(frozen=True, slots=True)
class BufferedEvent:
    event: StationEvent
    topic: str
    received_at_ms: int

    def to_stream_fields(self) -> dict[str, str]:
        normalized = self.event.model_dump(mode="json")
        return {
            "estacao_id": self.event.estacao_id,
            "unix_time": (
                str(self.event.unix_time)
                if self.event.unix_time is not None
                else "null"
            ),
            "received_at": str(self.received_at_ms),
            "topic": self.topic,
            "payload": json.dumps(
                normalized,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        }

