from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Sequence
from typing import Any, Protocol

from station_ingest.logging_config import log_event
from station_ingest.metrics import Metrics
from station_ingest.models import BufferedEvent
from station_ingest.retention import enforce_once


class RedisPipeline(Protocol):
    def xadd(self, name: str, fields: dict[str, str]) -> Any: ...

    async def execute(self) -> list[Any]: ...


class RedisClient(Protocol):
    def pipeline(self, transaction: bool = False) -> RedisPipeline: ...

    async def eval(self, script: str, numkeys: int, *args: Any) -> Any: ...


class RedisStreamWriter:
    def __init__(
        self,
        redis: RedisClient,
        stream: str,
        queue: asyncio.Queue[BufferedEvent],
        metrics: Metrics,
        *,
        batch_size: int,
        flush_interval_seconds: float,
        retry_initial_seconds: float,
        retry_max_seconds: float,
        logger: logging.Logger,
    ) -> None:
        self.redis = redis
        self.stream = stream
        self.queue = queue
        self.metrics = metrics
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.retry_initial_seconds = retry_initial_seconds
        self.retry_max_seconds = retry_max_seconds
        self.logger = logger

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set() or not self.queue.empty():
            batch = await self._collect_batch(stop_event)
            if not batch:
                continue
            await self._persist_with_retry(batch)
            now_ms = time.time_ns() // 1_000_000
            for event in batch:
                self.metrics.record_latency(max(0, now_ms - event.received_at_ms))
                if event.confirm is not None:
                    if event.confirm():
                        self.metrics.mqtt_acked += 1
                    else:
                        self.metrics.mqtt_unconfirmed += 1
                self.queue.task_done()
            self.metrics.persisted += len(batch)

    async def _collect_batch(self, stop_event: asyncio.Event) -> list[BufferedEvent]:
        if stop_event.is_set() and self.queue.empty():
            return []

        try:
            first = await asyncio.wait_for(
                self.queue.get(), timeout=self.flush_interval_seconds
            )
        except TimeoutError:
            return []

        batch = [first]
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.flush_interval_seconds

        while len(batch) < self.batch_size:
            try:
                batch.append(self.queue.get_nowait())
                continue
            except asyncio.QueueEmpty:
                pass

            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                batch.append(
                    await asyncio.wait_for(self.queue.get(), timeout=remaining)
                )
            except TimeoutError:
                break
        return batch

    async def _persist_with_retry(self, batch: Sequence[BufferedEvent]) -> None:
        delay = self.retry_initial_seconds
        while True:
            try:
                pipeline = self.redis.pipeline(transaction=False)
                for event in batch:
                    pipeline.xadd(self.stream, event.to_stream_fields())
                results = await pipeline.execute()
                if len(results) != len(batch) or any(
                    not isinstance(result, str | bytes) or not result
                    for result in results
                ):
                    raise RuntimeError("Redis returned an incomplete XADD result")
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.metrics.redis_retries += 1
                log_event(
                    self.logger,
                    "redis_batch_failed",
                    level=logging.ERROR,
                    batch_size=len(batch),
                    retry_in_seconds=delay,
                    error=str(exc),
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.retry_max_seconds)


async def enforce_retention_once(
    redis: RedisClient,
    stream: str,
    retention_seconds: int,
) -> int:
    return (await enforce_once(redis, stream, retention_seconds)).removed


async def run_retention(
    redis: RedisClient,
    stream: str,
    retention_seconds: int,
    interval_seconds: float,
    stop_event: asyncio.Event,
    logger: logging.Logger,
    *,
    required_groups: list[str] | None = None,
    trim_limit: int = 10000,
    max_groups: int = 128,
) -> None:
    while not stop_event.is_set():
        try:
            result = await enforce_once(
                redis,
                stream,
                retention_seconds,
                required_groups=required_groups,
                trim_limit=trim_limit,
                max_groups=max_groups,
            )
            log_event(
                logger,
                "redis_retention_status",
                removed=result.removed,
                boundary=result.boundary,
                reason=result.reason,
                blocking_group=result.blocking_group,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log_event(
                logger,
                "redis_retention_failed",
                level=logging.ERROR,
                error_type=type(exc).__name__,
            )

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            pass
