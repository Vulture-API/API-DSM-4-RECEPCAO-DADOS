import pytest

from station_ingest.models import BufferedEvent
from station_ingest.parsing import ParseFailure, parse_message


def test_parse_valid_message() -> None:
    result = parse_message(
        b'{"estacao_id":"e-1","unix_time":1700000000,"rain":2.1}',
        "estacoes/e-1/dados",
        1024,
        received_at_ms=321,
    )

    assert isinstance(result, BufferedEvent)
    assert result.received_at_ms == 321
    assert result.event.model_dump()["rain"] == 2.1


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (b"not-json", "invalid_json"),
        (b"[]", "payload_not_object"),
        (b'{"unix_time":1}', "invalid_or_missing_station_id"),
        (b'{"estacao_id":""}', "invalid_or_missing_station_id"),
    ],
)
def test_invalid_payload_is_rejected(payload: bytes, reason: str) -> None:
    result = parse_message(payload, "topic", 1024)
    assert result == ParseFailure(reason)


def test_payload_size_is_checked_before_json_decode() -> None:
    result = parse_message(b'{"estacao_id":"e-1"}', "topic", 3)
    assert result == ParseFailure("payload_too_large")

