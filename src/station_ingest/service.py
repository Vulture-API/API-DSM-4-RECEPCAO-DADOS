from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Iterable

from redis.asyncio import Redis

from station_ingest.config import Settings
from station_ingest.logging_config import log_event
from station_ingest.metrics import Metrics, report_metrics
from station_ingest.models import BufferedEvent
from station_ingest.mqtt_consumer import MqttConsumer
from station_ingest.redis_writer import RedisStreamWriter, run_retention


def install_signal_handlers(
    stop_event: asyncio.Event,
    signals: Iterable[signal.Signals] = (signal.SIGINT, signal.SIGTERM),
) -> None:
    loop = asyncio.get_running_loop()
    for sig in signals:
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:  # pragma: no cover - Windows fallback
            pass


async def run_service(settings: Settings) -> None:
    logger = logging.getLogger("station_ingest")
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)

    queue: asyncio.Queue[BufferedEvent] = asyncio.Queue(
        maxsize=settings.queue_max_size
    )
    metrics = Metrics()
    redis = Redis.from_url(
        settings.redis_url,
        max_connections=settings.redis_max_connections,
        socket_timeout=settings.redis_socket_timeout_seconds,
        socket_connect_timeout=settings.redis_socket_timeout_seconds,
        health_check_interval=30,
        decode_responses=True,
    )
    writer = RedisStreamWriter(
        redis,
        settings.redis_stream,
        queue,
        metrics,
        batch_size=settings.batch_size,
        flush_interval_seconds=settings.flush_interval_ms / 1000,
        retry_initial_seconds=settings.retry_initial_seconds,
        retry_max_seconds=settings.retry_max_seconds,
        logger=logger,
    )
    mqtt = MqttConsumer(settings, queue, metrics, logger)

    tasks = {
        "mqtt": asyncio.create_task(mqtt.run(stop_event)),
        "writer": asyncio.create_task(writer.run(stop_event)),
        "retention": asyncio.create_task(
            run_retention(
                redis,
                settings.redis_stream,
                settings.retention_days * 24 * 60 * 60,
                settings.retention_interval_seconds,
                stop_event,
                logger,
            )
        ),
        "metrics": asyncio.create_task(
            report_metrics(
                metrics,
                queue,
                stop_event,
                settings.metrics_interval_seconds,
                logger,
            )
        ),
    }
    stop_waiter = asyncio.create_task(stop_event.wait())
    failure: BaseException | None = None

    log_event(
        logger,
        "service_started",
        mqtt_topic=settings.mqtt_topic,
        redis_stream=settings.redis_stream,
        queue_max_size=settings.queue_max_size,
        batch_size=settings.batch_size,
    )
    try:
        done, _ = await asyncio.wait(
            {*tasks.values(), stop_waiter},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for name, task in tasks.items():
            if task in done and not task.cancelled():
                try:
                    task.result()
                except BaseException as exc:
                    failure = exc
                    log_event(
                        logger,
                        "service_task_failed",
                        level=logging.CRITICAL,
                        task=name,
                        error=str(exc),
                        exc_info=True,
                    )
                else:
                    failure = RuntimeError(f"task {name} exited unexpectedly")
                break
        stop_event.set()

        # Stop ingress before checking queue.join(), otherwise the MQTT task
        # could enqueue one final message just after the queue looked empty.
        try:
            await asyncio.wait_for(
                asyncio.shield(tasks["mqtt"]),
                timeout=min(5.0, settings.shutdown_timeout_seconds),
            )
        except TimeoutError:
            tasks["mqtt"].cancel()
            await asyncio.gather(tasks["mqtt"], return_exceptions=True)
        except BaseException as exc:
            if failure is None:
                failure = exc

        try:
            await asyncio.wait_for(
                queue.join(), timeout=settings.shutdown_timeout_seconds
            )
        except TimeoutError:
            log_event(
                logger,
                "shutdown_flush_timed_out",
                level=logging.ERROR,
                queue_depth=queue.qsize(),
            )
    finally:
        stop_event.set()
        stop_waiter.cancel()
        for task in tasks.values():
            task.cancel()
        await asyncio.gather(stop_waiter, *tasks.values(), return_exceptions=True)
        await redis.aclose()
        log_event(
            logger,
            "service_stopped",
            received=metrics.received,
            persisted=metrics.persisted,
            discarded=sum(metrics.discarded.values()),
            queue_depth=queue.qsize(),
        )

    if failure:
        raise failure
