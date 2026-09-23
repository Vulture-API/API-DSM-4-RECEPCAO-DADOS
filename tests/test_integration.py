import asyncio
import json
import os
import uuid

import aiomqtt
import pytest
from redis.asyncio import Redis

from station_ingest.config import Settings
from station_ingest.logging_config import configure_logging
from station_ingest.metrics import Metrics
from station_ingest.models import BufferedEvent
from station_ingest.mqtt_consumer import MqttConsumer
from station_ingest.redis_writer import RedisStreamWriter


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1",
        reason="set RUN_INTEGRATION=1 with MQTT and Redis endpoints",
    ),
]


@pytest.mark.asyncio
async def test_real_mqtt_to_redis_round_trip() -> None:
    suffix = uuid.uuid4().hex
    settings = Settings(
        _env_file=None,
        mqtt_host=os.getenv("TEST_MQTT_HOST", "localhost"),
        mqtt_port=int(os.getenv("TEST_MQTT_PORT", "1883")),
        mqtt_topic=f"integration/{suffix}/#",
        mqtt_client_id=f"station-ingest-test-{suffix}",
        redis_url=os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15"),
        redis_stream=f"test:telemetry:{suffix}",
        flush_interval_ms=10,
        retry_initial_seconds=0.01,
        retry_max_seconds=0.1,
    )
    configure_logging("WARNING")
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    queue: asyncio.Queue[BufferedEvent] = asyncio.Queue()
    metrics = Metrics()
    stop = asyncio.Event()
    writer = RedisStreamWriter(
        redis,
        settings.redis_stream,
        queue,
        metrics,
        batch_size=10,
        flush_interval_seconds=0.01,
        retry_initial_seconds=0.01,
        retry_max_seconds=0.1,
        logger=__import__("logging").getLogger("integration"),
    )
    consumer = MqttConsumer(
        settings,
        queue,
        metrics,
        __import__("logging").getLogger("integration"),
    )
    writer_task = asyncio.create_task(writer.run(stop))
    consumer_task = asyncio.create_task(consumer.run(stop))

    try:
        await asyncio.sleep(0.5)
        async with aiomqtt.Client(
            hostname=settings.mqtt_host,
            port=settings.mqtt_port,
        ) as publisher:
            await publisher.publish(
                f"integration/{suffix}/data",
                payload=json.dumps(
                    {"estacao_id": "integration-1", "unix_time": 1}
                ),
                qos=1,
            )

        async with asyncio.timeout(5):
            while await redis.xlen(settings.redis_stream) < 1:
                await asyncio.sleep(0.05)

        entries = await redis.xrange(settings.redis_stream)
        assert entries[0][1]["estacao_id"] == "integration-1"
    finally:
        stop.set()
        await asyncio.wait_for(queue.join(), timeout=2)
        for task in (writer_task, consumer_task):
            task.cancel()
        await asyncio.gather(writer_task, consumer_task, return_exceptions=True)
        await redis.delete(settings.redis_stream)
        await redis.aclose()

