import asyncio
import json
import logging

import pytest

from station_ingest.logging_config import JsonFormatter, configure_logging, log_event
from station_ingest.metrics import Metrics, report_metrics


class CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_json_formatter_includes_structured_fields() -> None:
    logger = logging.getLogger("test.formatter")
    record = logger.makeRecord(
        logger.name, logging.WARNING, __file__, 1, "queue_full", None, None
    )
    record.structured_fields = {"queue_depth": 10}

    payload = json.loads(JsonFormatter().format(record))

    assert payload["level"] == "WARNING"
    assert payload["logger"] == "test.formatter"
    assert payload["event"] == "queue_full"
    assert payload["queue_depth"] == 10
    assert "timestamp" in payload


def test_json_formatter_includes_exception() -> None:
    logger = logging.getLogger("test.formatter")
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = logger.makeRecord(
            logger.name, logging.ERROR, __file__, 1, "failed", None, sys.exc_info()
        )

    payload = json.loads(JsonFormatter().format(record))

    assert "ValueError: boom" in payload["exception"]


def test_configure_logging_replaces_root_handlers() -> None:
    root = logging.getLogger()
    previous_handlers, previous_level = root.handlers[:], root.level
    try:
        configure_logging("debug")

        assert root.level == logging.DEBUG
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JsonFormatter)
    finally:
        root.handlers[:] = previous_handlers
        root.setLevel(previous_level)


def test_metrics_counts_discards_by_reason() -> None:
    metrics = Metrics()
    metrics.record_discard("invalid_json")
    metrics.record_discard("invalid_json")
    metrics.record_discard("too_large")

    assert metrics.discarded == {"invalid_json": 2, "too_large": 1}


@pytest.mark.asyncio
async def test_report_metrics_logs_periodically_until_stopped() -> None:
    logger = logging.getLogger("test.metrics")
    handler = CaptureHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    metrics = Metrics()
    metrics.received = 5
    metrics.persisted = 3
    metrics.record_discard("invalid_json")
    queue: asyncio.Queue[object] = asyncio.Queue()
    await queue.put(object())
    stop_event = asyncio.Event()

    task = asyncio.create_task(report_metrics(metrics, queue, stop_event, 0.01, logger))
    await asyncio.sleep(0.05)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)
    logger.removeHandler(handler)

    events = [r for r in handler.records if r.getMessage() == "ingest_metrics"]
    assert events
    fields = events[0].structured_fields
    assert fields["received"] == 5
    assert fields["discarded"] == 1
    assert fields["discarded_by_reason"] == {"invalid_json": 1}
    assert fields["queue_depth"] == 1


def test_log_event_passes_level_and_fields() -> None:
    logger = logging.getLogger("test.log_event")
    handler = CaptureHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    log_event(logger, "something", level=logging.ERROR, station="x")
    logger.removeHandler(handler)

    assert handler.records[0].levelno == logging.ERROR
    assert handler.records[0].structured_fields == {"station": "x"}
