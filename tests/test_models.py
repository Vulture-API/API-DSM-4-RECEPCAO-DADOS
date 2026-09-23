import json

import pytest
from pydantic import ValidationError

from station_ingest.models import BufferedEvent, StationEvent


def test_station_event_preserves_dynamic_fields() -> None:
    event = StationEvent.model_validate(
        {
            "estacao_id": " station-1 ",
            "unix_time": 1_700_000_000,
            "temperature": 21.5,
            "status": {"battery": 88},
        }
    )

    assert event.estacao_id == "station-1"
    assert event.unix_time == 1_700_000_000
    assert event.model_dump()["temperature"] == 21.5
    assert event.model_dump()["status"] == {"battery": 88}


@pytest.mark.parametrize("value", [None, "1700000000", -1, True, 2.5, {}])
def test_invalid_timestamp_becomes_null(value: object) -> None:
    event = StationEvent.model_validate(
        {"estacao_id": "station-1", "unix_time": value}
    )
    assert event.unix_time is None


@pytest.mark.parametrize("value", [None, "", "   ", 42])
def test_station_id_must_be_non_empty_string(value: object) -> None:
    with pytest.raises(ValidationError):
        StationEvent.model_validate({"estacao_id": value})


def test_stream_fields_contain_normalized_payload() -> None:
    event = StationEvent.model_validate(
        {"estacao_id": "e-1", "unix_time": "bad", "humidity": 40}
    )
    buffered = BufferedEvent(event=event, topic="stations/e-1", received_at_ms=123)

    fields = buffered.to_stream_fields()

    assert fields["estacao_id"] == "e-1"
    assert fields["unix_time"] == "null"
    assert fields["received_at"] == "123"
    assert json.loads(fields["payload"]) == {
        "estacao_id": "e-1",
        "unix_time": None,
        "humidity": 40,
    }

