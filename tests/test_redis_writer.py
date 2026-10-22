import asyncio

import pytest

from station_ingest.metrics import Metrics
from station_ingest.models import BufferedEvent, StationEvent
from station_ingest.redis_writer import RedisStreamWriter, enforce_retention_once


class FakePipeline:
    def __init__(self, owner: "FakeRedis") -> None:
        self.owner = owner
        self.commands: list[tuple[str, dict[str, str]]] = []

    def xadd(self, name: str, fields: dict[str, str]) -> "FakePipeline":
        self.commands.append((name, fields))
        return self

    async def execute(self) -> list[str]:
        self.owner.execute_attempts += 1
        if self.owner.failures:
            self.owner.failures -= 1
            raise ConnectionError("temporary failure")
        self.owner.batches.append(self.commands)
        return [f"{index}-0" for index, _ in enumerate(self.commands)]


class FakeRedis:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.execute_attempts = 0
        self.batches: list[list[tuple[str, dict[str, str]]]] = []
        self.trim_call: tuple[str, str, bool] | None = None

    def pipeline(self, transaction: bool = False) -> FakePipeline:
        assert transaction is False
        return FakePipeline(self)

    async def eval(self, script, count, stream, age, groups, limit, max_groups):
        self.trim_call = (stream, age, groups, limit, max_groups)
        return [3, "1699999940500-0", "age", ""]


def buffered(station_id: str) -> BufferedEvent:
    return BufferedEvent(
        event=StationEvent(estacao_id=station_id, unix_time=1),
        topic="topic",
        received_at_ms=1,
    )


@pytest.mark.asyncio
async def test_writer_batches_in_queue_order() -> None:
    redis = FakeRedis()
    queue: asyncio.Queue[BufferedEvent] = asyncio.Queue()
    metrics = Metrics()
    for station_id in ("a", "b", "c"):
        queue.put_nowait(buffered(station_id))
    stop = asyncio.Event()
    stop.set()
    writer = RedisStreamWriter(
        redis,
        "telemetry:ingest",
        queue,
        metrics,
        batch_size=2,
        flush_interval_seconds=0.01,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.002,
        logger=__import__("logging").getLogger("test"),
    )

    await writer.run(stop)

    assert [[item[1]["estacao_id"] for item in batch] for batch in redis.batches] == [
        ["a", "b"],
        ["c"],
    ]
    assert metrics.persisted == 3
    assert queue.empty()


@pytest.mark.asyncio
async def test_writer_retries_without_losing_batch() -> None:
    redis = FakeRedis(failures=2)
    queue: asyncio.Queue[BufferedEvent] = asyncio.Queue()
    queue.put_nowait(buffered("a"))
    metrics = Metrics()
    writer = RedisStreamWriter(
        redis,
        "telemetry:ingest",
        queue,
        metrics,
        batch_size=10,
        flush_interval_seconds=0.001,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.002,
        logger=__import__("logging").getLogger("test"),
    )
    stop = asyncio.Event()
    stop.set()

    await writer.run(stop)

    assert redis.execute_attempts == 3
    assert metrics.redis_retries == 2
    assert len(redis.batches) == 1
    assert redis.batches[0][0][1]["estacao_id"] == "a"


@pytest.mark.asyncio
async def test_bounded_queue_applies_backpressure() -> None:
    queue: asyncio.Queue[int] = asyncio.Queue(maxsize=1)
    await queue.put(1)
    blocked_put = asyncio.create_task(queue.put(2))
    await asyncio.sleep(0)
    assert not blocked_put.done()

    assert await queue.get() == 1
    queue.task_done()
    await blocked_put
    assert await queue.get() == 2


@pytest.mark.asyncio
async def test_retention_delegates_to_atomic_safe_policy() -> None:
    redis = FakeRedis()

    removed = await enforce_retention_once(redis, "stream", 60)

    assert removed == 3
    assert redis.trim_call == ("stream", 60, '["postgres-persister"]', 10000, 128)


@pytest.mark.asyncio
async def test_ack_follows_successful_xadd_and_retry():
    from dataclasses import replace

    redis = FakeRedis(failures=1)
    queue = asyncio.Queue()
    attempts_at_ack = []
    event = replace(
        buffered("a"),
        confirm=lambda: attempts_at_ack.append(redis.execute_attempts) or True,
    )
    queue.put_nowait(event)
    metrics = Metrics()
    writer = RedisStreamWriter(
        redis,
        "stream",
        queue,
        metrics,
        batch_size=1,
        flush_interval_seconds=0.001,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.002,
        logger=__import__("logging").getLogger("test"),
    )
    stop = asyncio.Event()
    stop.set()
    await writer.run(stop)
    assert attempts_at_ack == [2]
    assert metrics.mqtt_acked == 1
    assert metrics.latency_count == 1


@pytest.mark.asyncio
async def test_cancelled_write_does_not_ack():
    from dataclasses import replace

    redis = FakeRedis(failures=1000)
    queue = asyncio.Queue()
    acks = []
    queue.put_nowait(replace(buffered("a"), confirm=lambda: acks.append(True) or True))
    writer = RedisStreamWriter(
        redis,
        "stream",
        queue,
        Metrics(),
        batch_size=1,
        flush_interval_seconds=0.001,
        retry_initial_seconds=10,
        retry_max_seconds=10,
        logger=__import__("logging").getLogger("test"),
    )
    task = asyncio.create_task(writer.run(asyncio.Event()))
    async with asyncio.timeout(1):
        while redis.execute_attempts == 0:
            await asyncio.sleep(0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert acks == []


@pytest.mark.asyncio
async def test_incomplete_pipeline_result_is_retried_before_ack():
    from dataclasses import replace
    from unittest.mock import AsyncMock, Mock

    redis, pipeline = Mock(), Mock()
    redis.pipeline.return_value = pipeline
    # The first call may have written a prefix of the batch. Retrying can
    # duplicate entries, but none of the MQTT receipts may be confirmed yet.
    pipeline.execute = AsyncMock(side_effect=[["1-0"], ["2-0", "3-0"]])
    queue, stop, metrics, acks = asyncio.Queue(), asyncio.Event(), Metrics(), []
    for station in ("a", "b"):
        queue.put_nowait(
            replace(
                buffered(station),
                confirm=lambda: acks.append(pipeline.execute.await_count) or True,
            )
        )
    writer = RedisStreamWriter(
        redis,
        "stream",
        queue,
        metrics,
        batch_size=2,
        flush_interval_seconds=0.001,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.002,
        logger=__import__("logging").getLogger("test"),
    )
    stop.set()
    await writer.run(stop)
    assert acks == [2, 2]
    assert metrics.redis_retries == 1
    assert metrics.persisted == 2


@pytest.mark.asyncio
async def test_failed_pipeline_does_not_log_command_payload(caplog):
    import logging
    from unittest.mock import AsyncMock, Mock

    from redis.exceptions import ResponseError

    redis, pipeline = Mock(), Mock()
    redis.pipeline.return_value = pipeline
    pipeline.execute = AsyncMock(
        side_effect=[
            ResponseError(
                "Command # 1 XADD payload PRIVATE_MEASUREMENT caused an error"
            ),
            ["1-0"],
        ]
    )
    queue, stop = asyncio.Queue(), asyncio.Event()
    queue.put_nowait(buffered("station"))
    stop.set()
    writer = RedisStreamWriter(
        redis,
        "stream",
        queue,
        Metrics(),
        batch_size=1,
        flush_interval_seconds=0.001,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.002,
        logger=logging.getLogger("test"),
    )
    with caplog.at_level(logging.ERROR):
        await writer.run(stop)
    from station_ingest.logging_config import JsonFormatter

    rendered = "\n".join(JsonFormatter().format(record) for record in caplog.records)
    assert "PRIVATE_MEASUREMENT" not in rendered
    assert "ResponseError" in rendered
