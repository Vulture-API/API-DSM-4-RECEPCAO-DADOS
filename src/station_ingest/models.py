from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StationEvent(BaseModel):
    """Validated station event while preserving sensor-specific fields."""

    model_config = ConfigDict(extra="allow")

    estacao_id: str = Field(min_length=1)
    unix_time: Any = None

    @field_validator("estacao_id", mode="before")
    @classmethod
    def validate_station_id(cls, value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("estacao_id must be a non-empty string")
        return value


@dataclass(frozen=True, slots=True)
class BufferedEvent:
    event: StationEvent
    topic: str
    received_at_ms: int
    confirm: Callable[[], bool] | None = field(default=None, compare=False, repr=False)
    original_payload: str | None = field(default=None, repr=False)

    def to_stream_fields(self) -> dict[str, str]:
        original = self.event.model_dump(mode="json", exclude_unset=True)
        unix_time = self.event.unix_time
        return {
            "estacao_id": self.event.estacao_id.strip(),
            "unix_time": (
                str(unix_time) if type(unix_time) is int and unix_time >= 0 else "null"
            ),
            "received_at": str(self.received_at_ms),
            "topic": self.topic,
            "payload": self.original_payload
            if self.original_payload is not None
            else json.dumps(
                original,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        }
