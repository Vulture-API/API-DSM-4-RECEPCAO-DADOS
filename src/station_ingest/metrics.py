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
        self.mqtt_acked = 0
        self.mqtt_unconfirmed = 0
        self.mqtt_saturations = 0
        self.bridge_depth = 0
        self.latency_count = 0
        self.latency_total_ms = 0
        self.latency_max_ms = 0
        self.discarded: Counter[str] = Counter()

    def record_discard(self, reason: str) -> None:
        self.discarded[reason] += 1

    def record_latency(self, milliseconds: int) -> None:
        self.latency_count += 1
        self.latency_total_ms += milliseconds
        self.latency_max_ms = max(self.latency_max_ms, milliseconds)


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
                mqtt_acked=metrics.mqtt_acked,
                mqtt_unconfirmed=metrics.mqtt_unconfirmed,
                mqtt_saturations=metrics.mqtt_saturations,
                latency_mean_ms=round(
                    metrics.latency_total_ms / max(1, metrics.latency_count), 2
                ),
                latency_max_ms=metrics.latency_max_ms,
                queue_depth=queue.qsize(),
                bridge_depth=metrics.bridge_depth,
                persisted_per_second=round(persisted_delta / elapsed, 2),
            )
            last_time = now
            last_persisted = metrics.persisted
