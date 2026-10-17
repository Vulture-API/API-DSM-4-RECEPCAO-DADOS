import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import Mock

import paho.mqtt.client as mqtt
import pytest

from station_ingest.config import Settings
from station_ingest.metrics import Metrics
from station_ingest.mqtt_consumer import MqttConsumer, Session, create_tls_context


def make(**kwargs):
    settings = Settings(
        _env_file=None,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.002,
        shutdown_timeout_seconds=0.02,
        **kwargs,
    )
    queue = asyncio.Queue(maxsize=settings.queue_max_size)
    metrics = Metrics()
    return (
        MqttConsumer(settings, queue, metrics, logging.getLogger("test")),
        queue,
        metrics,
    )


def reserve(consumer):
    consumer.bridge_slots.acquire()
    consumer.metrics.bridge_depth += 1


class Client:
    def __init__(self, **kwargs):
        assert kwargs["manual_ack"] is True
        self.kwargs = kwargs
        self.live = True
        self.acks = []
        self.stopped = False

    def is_connected(self):
        return self.live

    def ack(self, mid, qos):
        self.acks.append((mid, qos))
        return mqtt.MQTT_ERR_SUCCESS

    def connect_async(self, *_):
        pass

    def subscribe(self, *_args, **_kwargs):
        return (mqtt.MQTT_ERR_SUCCESS, 1)

    def loop_start(self):
        self.on_connect(self, None, None, SimpleNamespace(is_failure=False), None)
        self.on_subscribe(self, None, 1, [], None)

    def disconnect(self):
        self.live = False
        self.on_disconnect(self, None, None, None, None)

    def loop_stop(self):
        self.stopped = True


@pytest.mark.asyncio
async def test_valid_delivery_is_only_confirmed_by_writer():
    consumer, queue, metrics = make()
    client = Client(manual_ack=True)
    session = Session(client)
    session.active = True
    reserve(consumer)
    consumer._deliver(
        session, b'{"estacao_id":"a","unix_time":"bad"}', "topic", 7, 1, 123, Mock()
    )
    event = await queue.get()
    assert client.acks == []
    assert event.received_at_ms == 123
    assert event.event.unix_time == "bad"
    assert event.confirm()
    assert client.acks == [(7, 1)]
    assert metrics.received == 1
    session.invalidate()
    assert not event.confirm()
    assert client.acks == [(7, 1)]
    queue.task_done()


@pytest.mark.asyncio
async def test_invalid_envelope_is_acked_and_counted():
    consumer, queue, metrics = make()
    client = Client(manual_ack=True)
    session = Session(client)
    session.active = True
    reserve(consumer)
    consumer._deliver(session, b"bad", "topic", 8, 1, 123, Mock())
    assert queue.empty()
    assert client.acks == [(8, 1)]
    assert metrics.discarded["invalid_json"] == 1


@pytest.mark.asyncio
async def test_saturation_disconnects_without_confirming_valid_delivery():
    consumer, queue, metrics = make(queue_max_size=1, batch_size=1)
    queue.put_nowait(object())
    client = Client(manual_ack=True)
    session = Session(client)
    session.active = True
    disconnect = Mock(side_effect=session.invalidate)
    reserve(consumer)
    consumer._deliver(session, b'{"estacao_id":"a"}', "topic", 7, 1, 1, disconnect)
    assert client.acks == []
    disconnect.assert_called_once()
    assert metrics.mqtt_saturations == 1


@pytest.mark.asyncio
async def test_stale_queued_callback_does_not_admit_or_ack():
    consumer, queue, _ = make()
    session = Session(Client(manual_ack=True))
    session.invalidate()
    reserve(consumer)
    consumer._deliver(session, b'{"estacao_id":"a"}', "topic", 1, 1, 1, Mock())
    assert queue.empty()
    assert not session.confirm(1, 1)


@pytest.mark.asyncio
async def test_run_flushes_before_disconnect_and_stops_network():
    consumer, queue, _ = make()
    clients = []

    def factory(**kwargs):
        client = Client(**kwargs)
        clients.append(client)
        return client

    consumer.client_factory = factory
    stop = asyncio.Event()
    task = asyncio.create_task(consumer.run(stop))
    await asyncio.wait_for(consumer.connected.wait(), 1)
    clients[0].on_message(
        None,
        None,
        SimpleNamespace(payload=b'{"estacao_id":"a"}', topic="topic", mid=1, qos=1),
    )
    event = await asyncio.wait_for(queue.get(), 1)
    stop.set()
    await asyncio.sleep(0)
    assert clients[0].live
    assert event.confirm()
    queue.task_done()
    await asyncio.wait_for(task, 1)
    assert clients[0].stopped
    assert not clients[0].live


@pytest.mark.asyncio
async def test_reconnect_uses_new_client_and_invalidates_previous_receipt():
    consumer, queue, metrics = make()
    clients = []

    def factory(**kwargs):
        client = Client(**kwargs)
        clients.append(client)
        return client

    consumer.client_factory = factory
    stop = asyncio.Event()
    task = asyncio.create_task(consumer.run(stop))
    await asyncio.wait_for(consumer.connected.wait(), 1)
    clients[0].on_message(
        None,
        None,
        SimpleNamespace(payload=b'{"estacao_id":"a"}', topic="topic", mid=1, qos=1),
    )
    event = await asyncio.wait_for(queue.get(), 1)
    clients[0].disconnect()
    async with asyncio.timeout(1):
        while len(clients) < 2 or not consumer.connected.is_set():
            await asyncio.sleep(0.001)
    assert not event.confirm()
    assert clients[1].acks == []
    queue.task_done()
    stop.set()
    await asyncio.wait_for(task, 1)
    assert metrics.mqtt_reconnects == 1


@pytest.mark.asyncio
async def test_bridge_limit_disconnects_without_scheduling_unbounded_work():
    consumer, queue, metrics = make(mqtt_max_queued_incoming_messages=1)
    client = Client(manual_ack=True)
    consumer.client_factory = lambda **_: client
    reserve(consumer)
    stop = asyncio.Event()
    task = asyncio.create_task(consumer.run(stop))
    await asyncio.wait_for(consumer.connected.wait(), 1)
    client.on_message(
        None,
        None,
        SimpleNamespace(payload=b'{"estacao_id":"a"}', topic="topic", mid=1, qos=1),
    )
    await asyncio.sleep(0)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert metrics.mqtt_saturations == 1
    assert client.acks == []
    assert queue.empty()
    consumer.bridge_slots.release()


def test_qos_zero_has_no_ack_guarantee():
    session = Session(Client(manual_ack=True))
    session.active = True
    assert not session.confirm(1, 0)


def test_tls_and_credentials(monkeypatch):
    consumer, _, _ = make(
        mqtt_username="test",
        mqtt_password="secret",
        mqtt_tls=True,
        mqtt_tls_insecure=True,
        mqtt_tls_ca_file="ca",
        mqtt_tls_cert_file="cert",
        mqtt_tls_key_file="key",
    )
    context = Mock()
    monkeypatch.setattr("ssl.create_default_context", lambda **_: context)
    client = Mock()
    consumer.client_factory = lambda **_: client
    assert consumer._client() is client
    client.username_pw_set.assert_called_once_with("test", "secret")
    context.load_cert_chain.assert_called_once_with("cert", "key")
    assert context.check_hostname is False
    assert create_tls_context(Settings(_env_file=None)) is None


@pytest.mark.asyncio
async def test_rejected_subscription_never_reports_ready():
    consumer, _, _ = make()
    client = Client(manual_ack=True)
    session = Session(client)
    disconnected = asyncio.Event()
    consumer._bind_client(client, session, disconnected, asyncio.get_running_loop())
    client.on_subscribe(client, None, 1, [SimpleNamespace(is_failure=True)], None)
    await asyncio.sleep(0)
    assert disconnected.is_set()
    assert not consumer.connected.is_set()
    assert not session.active


@pytest.mark.asyncio
async def test_shutdown_deadline_leaves_unfinished_delivery_unacked():
    consumer, queue, _ = make()
    client = Client(manual_ack=True)
    consumer.client_factory = lambda **_: client
    stop = asyncio.Event()
    task = asyncio.create_task(consumer.run(stop))
    await asyncio.wait_for(consumer.connected.wait(), 1)
    client.on_message(
        None,
        None,
        SimpleNamespace(
            payload=b'{"estacao_id":"a"}',
            topic="topic",
            mid=1,
            qos=1,
        ),
    )
    event = await asyncio.wait_for(queue.get(), 1)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert client.acks == []
    assert client.stopped
    assert not event.confirm()
    queue.task_done()
