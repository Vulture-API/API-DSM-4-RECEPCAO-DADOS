import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest
import aiomqtt

from station_ingest.config import Settings
from station_ingest.metrics import Metrics
from station_ingest.models import BufferedEvent
from station_ingest.mqtt_consumer import MqttConsumer


@dataclass
class Message:
    payload: bytes
    topic: str


class Client:
    def __init__(self, messages: list[Message]) -> None:
        self._messages = messages
        self.subscribed: tuple[str, int] | None = None

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def subscribe(self, topic: str, qos: int) -> None:
        self.subscribed = (topic, qos)

    @property
    def messages(self) -> AsyncIterator[Message]:
        async def iterator() -> AsyncIterator[Message]:
            for message in self._messages:
                yield message

        return iterator()


@pytest.mark.asyncio
async def test_consumer_validates_and_queues_messages() -> None:
    client = Client(
        [
            Message(b"bad-json", "topic/a"),
            Message(b'{"estacao_id":"a","unix_time":1}', "topic/a"),
        ]
    )
    settings = Settings(_env_file=None)
    queue: asyncio.Queue[BufferedEvent] = asyncio.Queue()
    metrics = Metrics()
    consumer = MqttConsumer(
        settings,
        queue,
        metrics,
        logging.getLogger("test"),
        client_factory=lambda **_: client,
    )
    stop = asyncio.Event()

    await consumer._consume_connection(client, stop)

    assert metrics.received == 2
    assert metrics.discarded["invalid_json"] == 1
    assert queue.qsize() == 1
    assert (await queue.get()).event.estacao_id == "a"


@pytest.mark.asyncio
async def test_consumer_reconnects_after_connection_loss() -> None:
    stop = asyncio.Event()

    class DisconnectingClient(Client):
        @property
        def messages(self) -> AsyncIterator[Message]:
            async def iterator() -> AsyncIterator[Message]:
                raise aiomqtt.MqttError("connection lost")
                yield  # pragma: no cover

            return iterator()

    class StoppingClient(Client):
        @property
        def messages(self) -> AsyncIterator[Message]:
            async def iterator() -> AsyncIterator[Message]:
                stop.set()
                if False:
                    yield Message(b"", "")

            return iterator()

    clients = [DisconnectingClient([]), StoppingClient([])]
    settings = Settings(
        _env_file=None,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.002,
    )
    queue: asyncio.Queue[BufferedEvent] = asyncio.Queue()
    metrics = Metrics()
    consumer = MqttConsumer(
        settings,
        queue,
        metrics,
        logging.getLogger("test"),
        client_factory=lambda **_: clients.pop(0),
    )

    await asyncio.wait_for(consumer.run(stop), timeout=1)

    assert metrics.mqtt_reconnects == 1
    assert clients == []
