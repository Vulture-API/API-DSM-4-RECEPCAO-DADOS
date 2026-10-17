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
        await asyncio.wait_for(consumer.connected.wait(), 5)
        async with aiomqtt.Client(
            hostname=settings.mqtt_host,
            port=settings.mqtt_port,
        ) as publisher:
            await publisher.publish(
                f"integration/{suffix}/data",
                payload=json.dumps({"estacao_id": "integration-1", "unix_time": 1}),
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


@pytest.mark.parametrize("stored_before_crash", [False, True])
@pytest.mark.asyncio
async def test_killed_receiver_redelivers_unacked_message(stored_before_crash):
    """Real broker, SIGKILL and persistent session; never restart the broker."""
    import sys

    suffix = uuid.uuid4().hex
    topic, stream = f"test/{suffix}/data", f"test:{suffix}"
    client_id = f"crash-{suffix}"
    redis_url = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")
    host, port = (
        os.getenv("TEST_MQTT_HOST", "localhost"),
        int(os.getenv("TEST_MQTT_PORT", "1883")),
    )
    child_code = """
import asyncio, sys
from redis.asyncio import Redis
from station_ingest.config import Settings
from station_ingest.metrics import Metrics
from station_ingest.mqtt_consumer import MqttConsumer
import logging
async def main():
    host, port, topic, stream, client_id, url, store = sys.argv[1:]
    settings = Settings(_env_file=None, mqtt_host=host, mqtt_port=int(port), mqtt_topic=topic, mqtt_client_id=client_id)
    queue = asyncio.Queue(maxsize=10)
    worker = MqttConsumer(settings, queue, Metrics(), logging.getLogger('child'))
    asyncio.create_task(worker.run(asyncio.Event()))
    await worker.connected.wait()
    print('READY', flush=True)
    event = await queue.get()
    if store == 'yes':
        redis = Redis.from_url(url, decode_responses=True)
        await redis.xadd(stream, event.to_stream_fields())
    print('RECEIVED', flush=True)
    await asyncio.Event().wait()
asyncio.run(main())
"""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        child_code,
        host,
        str(port),
        topic,
        stream,
        client_id,
        redis_url,
        "yes" if stored_before_crash else "no",
        stdout=asyncio.subprocess.PIPE,
    )
    redis = Redis.from_url(redis_url, decode_responses=True)
    consumer_task = writer_task = None
    stop = asyncio.Event()
    try:
        assert await asyncio.wait_for(process.stdout.readline(), 5) == b"READY\n"
        async with aiomqtt.Client(hostname=host, port=port) as publisher:
            await publisher.publish(
                topic, b'{"estacao_id":"e-1","unix_time":"invalid","temp":21}', qos=1
            )
        assert await asyncio.wait_for(process.stdout.readline(), 5) == b"RECEIVED\n"
        process.kill()
        await asyncio.wait_for(process.wait(), 5)
        settings = Settings(
            _env_file=None,
            mqtt_host=host,
            mqtt_port=port,
            mqtt_topic=topic,
            mqtt_client_id=client_id,
            redis_url=redis_url,
            redis_stream=stream,
            retry_initial_seconds=0.01,
            retry_max_seconds=0.1,
        )
        queue = asyncio.Queue(maxsize=10)
        metrics = Metrics()
        consumer = MqttConsumer(
            settings, queue, metrics, __import__("logging").getLogger("test")
        )
        writer = RedisStreamWriter(
            redis,
            stream,
            queue,
            metrics,
            batch_size=10,
            flush_interval_seconds=0.01,
            retry_initial_seconds=0.01,
            retry_max_seconds=0.1,
            logger=__import__("logging").getLogger("test"),
        )
        writer_task = asyncio.create_task(writer.run(stop))
        consumer_task = asyncio.create_task(consumer.run(stop))
        async with asyncio.timeout(5):
            while metrics.mqtt_acked < 1:
                await asyncio.sleep(0.01)
        entries = await redis.xrange(stream)
        assert len(entries) == (2 if stored_before_crash else 1)
        assert all(
            json.loads(f["payload"])["unix_time"] == "invalid" for _, f in entries
        )
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
        stop.set()
        if consumer_task:
            await asyncio.wait_for(consumer_task, 5)
        if writer_task:
            await asyncio.wait_for(writer_task, 5)
        await redis.delete(stream)
        await redis.aclose()
