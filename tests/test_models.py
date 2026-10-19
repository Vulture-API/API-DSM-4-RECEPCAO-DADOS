import json

import pytest
from pydantic import ValidationError

from station_ingest.models import BufferedEvent, StationEvent


@pytest.mark.parametrize("value", [None, "1700000000", -1, True, 2.5, {}, [1]])
def test_timestamp_original_is_preserved(value):
    raw = {"estacao_id": " e-1 ", "unix_time": value, "extra": {"battery": 88}}
    event = BufferedEvent(StationEvent.model_validate(raw), "topic", 123)
    fields = event.to_stream_fields()
    assert fields["estacao_id"] == "e-1"
    assert fields["unix_time"] == "null"
    assert json.loads(fields["payload"]) == raw
    assert fields["received_at"] == "123"


def test_absent_timestamp_is_not_inserted_into_payload():
    raw = {"estacao_id": "e-1", "temp": 21.5}
    fields = BufferedEvent(
        StationEvent.model_validate(raw), "topic", 1
    ).to_stream_fields()
    assert fields["unix_time"] == "null"
    assert json.loads(fields["payload"]) == raw


def test_valid_timestamp_and_nested_fields():
    raw = {"estacao_id": "e-1", "unix_time": 0, "status": {"battery": 88}}
    fields = BufferedEvent(
        StationEvent.model_validate(raw), "topic", 1
    ).to_stream_fields()
    assert fields["unix_time"] == "0"
    assert json.loads(fields["payload"]) == raw


@pytest.mark.parametrize("value", [None, "", "   ", 42])
def test_station_id_must_be_non_empty_string(value):
    with pytest.raises(ValidationError):
        StationEvent.model_validate({"estacao_id": value})
