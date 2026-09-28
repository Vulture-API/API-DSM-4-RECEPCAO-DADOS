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

    async def time(self) -> tuple[int, int]:
        return (1_700_000_000, 500_000)

    async def xtrim(
        self, name: str, *, minid: str, approximate: bool
    ) -> int:
        self.trim_call = (name, minid, approximate)
        return 3


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
async def test_retention_uses_redis_clock() -> None:
    redis = FakeRedis()

    removed = await enforce_retention_once(redis, "stream", 60)

    assert removed == 3
    assert redis.trim_call == ("stream", "1699999940500-0", True)
