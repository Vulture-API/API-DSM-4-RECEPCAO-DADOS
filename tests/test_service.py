import asyncio
import logging
from unittest.mock import AsyncMock, Mock

import pytest

from station_ingest.config import Settings
from station_ingest.metrics import Metrics, report_metrics
from station_ingest.models import BufferedEvent, StationEvent
from station_ingest.service import run_service


@pytest.mark.asyncio
async def test_shutdown_keeps_writer_alive_until_admission_stops(monkeypatch):
    redis = Mock()
    redis.aclose = AsyncMock()
    pipeline = Mock()
    pipeline.execute = AsyncMock(return_value=["1-0"])
    redis.pipeline.return_value = pipeline
    monkeypatch.setattr("station_ingest.service.Redis.from_url", lambda *a, **k: redis)
    monkeypatch.setattr(
        "station_ingest.service.install_signal_handlers", lambda stop: None
    )
    confirmed = []

    class Consumer:
        def __init__(self, settings, queue, *_):
            self.queue = queue

        async def run(self, stop):
            await asyncio.sleep(0.005)
            stop.set()
            # A callback already scheduled before admission is closed can run
            # after the stop signal. The writer must still be available.
            await asyncio.sleep(0)
            await self.queue.put(
                BufferedEvent(
                    StationEvent(estacao_id="e-1"),
                    "topic",
                    123,
                    confirm=lambda: confirmed.append(True) or True,
                )
            )
            await self.queue.join()

    monkeypatch.setattr("station_ingest.service.MqttConsumer", Consumer)
    await asyncio.wait_for(
        run_service(
            Settings(
                _env_file=None,
                flush_interval_ms=1,
                shutdown_timeout_seconds=0.2,
                # Reception must never attempt a PostgreSQL connection.
                database_url="postgresql://unavailable:1/missing",
            )
        ),
        2,
    )
    assert confirmed == [True]
    pipeline.xadd.assert_called_once()
    redis.xtrim.assert_not_called()
    redis.aclose.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("raises", [False, True])
async def test_worker_failure_closes_resources_and_surfaces_failure(
    monkeypatch, raises
):
    redis = Mock(aclose=AsyncMock())
    monkeypatch.setattr("station_ingest.service.Redis.from_url", lambda *a, **k: redis)
    monkeypatch.setattr(
        "station_ingest.service.install_signal_handlers", lambda stop: None
    )

    class Consumer:
        def __init__(self, *_):
            pass

        async def run(self, stop):
            if raises:
                raise RuntimeError("network worker failed")

    monkeypatch.setattr("station_ingest.service.MqttConsumer", Consumer)
    with pytest.raises(RuntimeError, match="network worker failed|exited unexpectedly"):
        await run_service(Settings(_env_file=None, shutdown_timeout_seconds=0.01))
    redis.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_metrics_report_queue_depth_rejections_and_redis_latency(caplog):
    metrics, queue, stop = Metrics(), asyncio.Queue(), asyncio.Event()
    metrics.persisted = 2
    metrics.record_latency(4)
    metrics.record_latency(8)
    metrics.record_discard("invalid_json")
    queue.put_nowait(object())
    with caplog.at_level(logging.INFO):
        task = asyncio.create_task(
            report_metrics(metrics, queue, stop, 0.001, logging.getLogger("test"))
        )
        async with asyncio.timeout(1):
            while not caplog.records:
                await asyncio.sleep(0.001)
        stop.set()
        await task
    record = caplog.records[0]
    assert record.structured_fields["latency_mean_ms"] == 6
    assert record.structured_fields["latency_max_ms"] == 8
    assert record.structured_fields["queue_depth"] == 1
    assert record.structured_fields["discarded_by_reason"] == {"invalid_json": 1}
