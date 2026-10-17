import asyncio
import logging

import pytest

from station_ingest import service
from station_ingest.config import Settings


class FakeRedis:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


class CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.events.append(record)


@pytest.fixture
def fakes(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    redis = FakeRedis()
    state: dict[str, object] = {"redis": redis, "writer_error": None}

    class FakeMqtt:
        def __init__(self, *args: object) -> None:
            pass

        async def run(self, stop_event: asyncio.Event) -> None:
            await stop_event.wait()
            await asyncio.sleep(0.01)

    class FakeWriter:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        async def run(self, stop_event: asyncio.Event) -> None:
            error = state["writer_error"]
            if isinstance(error, BaseException):
                raise error
            await stop_event.wait()
            await asyncio.sleep(0.01)

    async def idle(*args: object) -> None:
        stop_event = next(a for a in args if isinstance(a, asyncio.Event))
        await stop_event.wait()
        await asyncio.sleep(0.01)

    def capture_stop(stop_event: asyncio.Event, *args: object) -> None:
        state["stop_event"] = stop_event

    monkeypatch.setattr(service.Redis, "from_url", lambda *a, **k: redis)
    monkeypatch.setattr(service, "MqttConsumer", FakeMqtt)
    monkeypatch.setattr(service, "RedisStreamWriter", FakeWriter)
    monkeypatch.setattr(service, "run_retention", idle)
    monkeypatch.setattr(service, "report_metrics", idle)
    monkeypatch.setattr(service, "install_signal_handlers", capture_stop)
    return state


def settings() -> Settings:
    return Settings(_env_file=None, shutdown_timeout_seconds=1)


@pytest.mark.asyncio
async def test_service_stops_cleanly_on_signal(fakes: dict[str, object]) -> None:
    task = asyncio.create_task(service.run_service(settings()))
    while "stop_event" not in fakes:
        await asyncio.sleep(0)

    stop_event = fakes["stop_event"]
    assert isinstance(stop_event, asyncio.Event)
    stop_event.set()
    await asyncio.wait_for(task, timeout=2)

    redis = fakes["redis"]
    assert isinstance(redis, FakeRedis) and redis.closed


@pytest.mark.asyncio
async def test_service_propagates_task_failure(fakes: dict[str, object]) -> None:
    fakes["writer_error"] = ConnectionError("redis caiu")
    logger = logging.getLogger("station_ingest")
    handler = CaptureHandler()
    logger.addHandler(handler)
    try:
        with pytest.raises(ConnectionError, match="redis caiu"):
            await asyncio.wait_for(service.run_service(settings()), timeout=2)
    finally:
        logger.removeHandler(handler)

    failed = [r for r in handler.events if r.getMessage() == "service_task_failed"]
    assert failed and failed[0].structured_fields["task"] == "writer"
    redis = fakes["redis"]
    assert isinstance(redis, FakeRedis) and redis.closed
