from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter

from station_ingest.logging_config import log_event


class Metrics:
    def __init__(self) -> None:
        self.received = 0
        self.persisted = 0
        self.redis_retries = 0
        self.mqtt_reconnects = 0
        self.discarded: Counter[str] = Counter()

    def record_discard(self, reason: str) -> None:
        self.discarded[reason] += 1


async def report_metrics(
    metrics: Metrics,
    queue: asyncio.Queue[object],
    stop_event: asyncio.Event,
    interval_seconds: float,
    logger: logging.Logger,
) -> None:
    last_time = time.monotonic()
    last_persisted = metrics.persisted
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            now = time.monotonic()
            elapsed = max(now - last_time, 0.001)
            persisted_delta = metrics.persisted - last_persisted
            log_event(
                logger,
                "ingest_metrics",
                received=metrics.received,
                persisted=metrics.persisted,
                discarded=sum(metrics.discarded.values()),
                discarded_by_reason=dict(metrics.discarded),
                redis_retries=metrics.redis_retries,
                mqtt_reconnects=metrics.mqtt_reconnects,
                queue_depth=queue.qsize(),
                persisted_per_second=round(persisted_delta / elapsed, 2),
            )
            last_time = now
            last_persisted = metrics.persisted

