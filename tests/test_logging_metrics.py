import asyncio
import itertools
import json
import logging
import sys
import types

import pytest

from station_ingest import metrics as metrics_module
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


class EventHandler(logging.Handler):
    """Avisa quando chega o primeiro registro com o evento esperado."""

    def __init__(self, event: str) -> None:
        super().__init__()
        self.event = event
        self.records: list[logging.LogRecord] = []
        self.seen = asyncio.Event()

    def emit(self, record: logging.LogRecord) -> None:
        if record.getMessage() == self.event:
            self.records.append(record)
            self.seen.set()


@pytest.mark.asyncio
async def test_report_metrics_logs_periodically_until_stopped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ticks = itertools.count(100.0, 2.0)
    monkeypatch.setattr(
        metrics_module, "time", types.SimpleNamespace(monotonic=lambda: next(ticks))
    )
    logger = logging.getLogger("test.metrics")
    handler = EventHandler("ingest_metrics")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    metrics = Metrics()
    metrics.received = 5
    metrics.persisted = 3
    metrics.record_discard("invalid_json")
    queue: asyncio.Queue[object] = asyncio.Queue()
    await queue.put(object())
    stop_event = asyncio.Event()

    try:
        task = asyncio.create_task(
            report_metrics(metrics, queue, stop_event, 0.01, logger)
        )
        await asyncio.wait_for(handler.seen.wait(), timeout=2)
        handler.seen.clear()
        metrics.persisted += 10
        await asyncio.wait_for(handler.seen.wait(), timeout=2)
        stop_event.set()
        await asyncio.wait_for(task, timeout=2)
    finally:
        logger.removeHandler(handler)

    fields = handler.records[0].structured_fields
    assert fields["received"] == 5
    assert fields["persisted"] == 3
    assert fields["discarded"] == 1
    assert fields["discarded_by_reason"] == {"invalid_json": 1}
    assert fields["queue_depth"] == 1
    assert fields["persisted_per_second"] == 0.0
    # 10 persistidas entre os dois relatórios, com 2 s de intervalo no relógio.
    assert handler.records[1].structured_fields["persisted_per_second"] == 5.0


def test_log_event_passes_level_and_fields() -> None:
    logger = logging.getLogger("test.log_event")
    handler = CaptureHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    log_event(logger, "something", level=logging.ERROR, station="x")
    logger.removeHandler(handler)

    assert handler.records[0].levelno == logging.ERROR
    assert handler.records[0].structured_fields == {"station": "x"}
