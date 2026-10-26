from __future__ import annotations

import asyncio
import logging
import ssl
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from typing import Any

import paho.mqtt.client as mqtt

from station_ingest.config import Settings
from station_ingest.logging_config import log_event
from station_ingest.metrics import Metrics
from station_ingest.models import BufferedEvent
from station_ingest.parsing import ParseFailure, parse_message


def create_tls_context(settings: Settings) -> ssl.SSLContext | None:
    if not settings.mqtt_tls:
        return None
    context = ssl.create_default_context(
        cafile=str(settings.mqtt_tls_ca_file) if settings.mqtt_tls_ca_file else None
    )
    if settings.mqtt_tls_cert_file and settings.mqtt_tls_key_file:
        context.load_cert_chain(
            str(settings.mqtt_tls_cert_file), str(settings.mqtt_tls_key_file)
        )
    if settings.mqtt_tls_insecure:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


class Session:
    """Receipts belong to this client instance, never a subsequent connection."""

    def __init__(self, client: Any) -> None:
        self.client = client
        self.lock = threading.RLock()
        self.active = False
        self.accepting = True

    def invalidate(self) -> None:
        with self.lock:
            self.active = False
            self.accepting = False

    def confirm(self, mid: int, qos: int) -> bool:
        with self.lock:
            if qos == 0 or not self.active or not self.client.is_connected():
                return False
            return self.client.ack(mid, qos) == mqtt.MQTT_ERR_SUCCESS


class MqttConsumer:
    def __init__(
        self,
        settings: Settings,
        queue: asyncio.Queue[BufferedEvent],
        metrics: Metrics,
        logger: logging.Logger,
        *,
        client_factory: Callable[..., Any] = mqtt.Client,
    ) -> None:
        self.settings, self.queue = settings, queue
        self.metrics, self.logger = metrics, logger
        self.client_factory = client_factory
        self.bridge_slots = threading.BoundedSemaphore(
            settings.mqtt_max_queued_incoming_messages
        )
        self.connected = asyncio.Event()
        self.bridge_lock = threading.Lock()

    def _client(self) -> Any:
        client = self.client_factory(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.settings.mqtt_client_id,
            protocol=mqtt.MQTTv311,
            clean_session=False,
            manual_ack=True,
            reconnect_on_failure=False,
        )
        client.connect_timeout = self.settings.redis_socket_timeout_seconds
        if self.settings.mqtt_username is not None:
            client.username_pw_set(
                self.settings.mqtt_username,
                self.settings.mqtt_password.get_secret_value()
                if self.settings.mqtt_password
                else None,
            )
        context = create_tls_context(self.settings)
        if context:
            client.tls_set_context(context)
        return client

    def _deliver(
        self,
        session: Session,
        payload: bytes,
        topic: str,
        mid: int,
        qos: int,
        received_at_ms: int,
        disconnect: Callable[[], None],
    ) -> None:
        try:
            if not session.accepting or not session.active:
                return
            self.metrics.received += 1
            event = parse_message(
                payload,
                topic,
                self.settings.max_payload_bytes,
                received_at_ms=received_at_ms,
            )
            if isinstance(event, ParseFailure):
                self.metrics.record_discard(event.reason)
                log_event(self.logger, "mqtt_message_discarded", reason=event.reason)
                session.confirm(mid, qos)
                return
            event = replace(event, confirm=lambda: session.confirm(mid, qos))
            try:
                self.queue.put_nowait(event)
            except asyncio.QueueFull:
                self._record_saturation()
                disconnect()
        finally:
            with self.bridge_lock:
                self.metrics.bridge_depth -= 1
            self.bridge_slots.release()

    def _bind_client(
        self,
        client: Any,
        session: Session,
        disconnected: asyncio.Event,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        def schedule(callback: Callable[..., None], *args: Any) -> None:
            try:
                loop.call_soon_threadsafe(callback, *args)
            except RuntimeError:
                session.invalidate()

        def disconnect() -> None:
            session.invalidate()
            client.disconnect()
            schedule(disconnected.set)

        def on_connect(
            _client: Any, _userdata: Any, _flags: Any, reason: Any, _properties: Any
        ) -> None:
            if reason.is_failure:
                disconnect()
                return
            with session.lock:
                session.active = True
            rc, _ = client.subscribe(self.settings.mqtt_topic, qos=1)
            if rc != mqtt.MQTT_ERR_SUCCESS:
                disconnect()
                return

        def on_subscribe(
            _client: Any, _userdata: Any, _mid: Any, reasons: Any, _properties: Any
        ) -> None:
            if any(reason.is_failure for reason in reasons):
                disconnect()
                return
            schedule(self.connected.set)

        def on_disconnect(*_args: Any) -> None:
            session.invalidate()
            schedule(disconnected.set)

        def on_message(_client: Any, _userdata: Any, message: Any) -> None:
            received_at_ms = time.time_ns() // 1_000_000
            if not session.accepting:
                return
            if not self.bridge_slots.acquire(blocking=False):
                schedule(self._record_saturation)
                disconnect()
                return
            with self.bridge_lock:
                self.metrics.bridge_depth += 1
            try:
                loop.call_soon_threadsafe(
                    self._deliver,
                    session,
                    bytes(message.payload),
                    str(message.topic),
                    message.mid,
                    message.qos,
                    received_at_ms,
                    disconnect,
                )
            except RuntimeError:
                with self.bridge_lock:
                    self.metrics.bridge_depth -= 1
                self.bridge_slots.release()
                disconnect()

        client.on_connect, client.on_disconnect = on_connect, on_disconnect
        client.on_connect_fail, client.on_message = on_disconnect, on_message
        client.on_subscribe = on_subscribe

    async def run(self, stop_event: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        delay = self.settings.retry_initial_seconds
        connected_once = False
        while not stop_event.is_set():
            client = self._client()
            session = Session(client)
            disconnected = asyncio.Event()
            self.connected.clear()

            self._bind_client(client, session, disconnected, loop)
            waiters: list[asyncio.Task[Any]] = []
            try:
                client.connect_async(
                    self.settings.mqtt_host,
                    self.settings.mqtt_port,
                    self.settings.mqtt_keepalive,
                )
                client.loop_start()
                waiters = [
                    asyncio.create_task(disconnected.wait()),
                    asyncio.create_task(stop_event.wait()),
                    asyncio.create_task(self.connected.wait()),
                ]
                await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
                if self.connected.is_set():
                    if connected_once:
                        self.metrics.mqtt_reconnects += 1
                    connected_once = True
                    delay = self.settings.retry_initial_seconds
                    log_event(
                        self.logger, "mqtt_connected", topic=self.settings.mqtt_topic
                    )
                    await asyncio.wait(waiters[:2], return_when=asyncio.FIRST_COMPLETED)
                if stop_event.is_set():
                    session.accepting = False
                    # Keep the socket alive while the writer flushes and sends ACKs.
                    try:
                        await asyncio.wait_for(
                            self.queue.join(), self.settings.shutdown_timeout_seconds
                        )
                    except TimeoutError:
                        log_event(
                            self.logger, "mqtt_shutdown_pending", level=logging.ERROR
                        )
            finally:
                session.invalidate()
                self.connected.clear()
                for waiter in waiters:
                    waiter.cancel()
                await asyncio.gather(*waiters, return_exceptions=True)
                client.disconnect()
                await asyncio.to_thread(client.loop_stop)
            if not stop_event.is_set():
                log_event(self.logger, "mqtt_reconnecting", retry_in_seconds=delay)
                try:
                    await asyncio.wait_for(stop_event.wait(), delay)
                except TimeoutError:
                    pass
                delay = min(delay * 2, self.settings.retry_max_seconds)

    def _record_saturation(self) -> None:
        self.metrics.mqtt_saturations += 1
        log_event(self.logger, "mqtt_saturated", queue_depth=self.queue.qsize())
