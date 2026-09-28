from __future__ import annotations

import asyncio
import logging
import ssl
from collections.abc import Callable
from typing import Any

import aiomqtt

from station_ingest.config import Settings
from station_ingest.logging_config import log_event
from station_ingest.metrics import Metrics
from station_ingest.models import BufferedEvent
from station_ingest.parsing import ParseFailure, parse_message


def create_tls_context(settings: Settings) -> ssl.SSLContext | None:
    if not settings.mqtt_tls:
        return None
    context = ssl.create_default_context(
        cafile=(
            str(settings.mqtt_tls_ca_file)
            if settings.mqtt_tls_ca_file
            else None
        )
    )
    if settings.mqtt_tls_cert_file and settings.mqtt_tls_key_file:
        context.load_cert_chain(
            certfile=str(settings.mqtt_tls_cert_file),
            keyfile=str(settings.mqtt_tls_key_file),
        )
    if settings.mqtt_tls_insecure:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


class MqttConsumer:
    def __init__(
        self,
        settings: Settings,
        queue: asyncio.Queue[BufferedEvent],
        metrics: Metrics,
        logger: logging.Logger,
        *,
        client_factory: Callable[..., Any] = aiomqtt.Client,
    ) -> None:
        self.settings = settings
        self.queue = queue
        self.metrics = metrics
        self.logger = logger
        self.client_factory = client_factory

    def _client(self) -> Any:
        password = (
            self.settings.mqtt_password.get_secret_value()
            if self.settings.mqtt_password
            else None
        )
        return self.client_factory(
            hostname=self.settings.mqtt_host,
            port=self.settings.mqtt_port,
            username=self.settings.mqtt_username,
            password=password,
            identifier=self.settings.mqtt_client_id,
            protocol=aiomqtt.ProtocolVersion.V311,
            clean_session=False,
            keepalive=self.settings.mqtt_keepalive,
            max_queued_incoming_messages=(
                self.settings.mqtt_max_queued_incoming_messages
            ),
            tls_context=create_tls_context(self.settings),
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        delay = self.settings.retry_initial_seconds
        connected_once = False
        while not stop_event.is_set():
            try:
                async with self._client() as client:
                    await client.subscribe(self.settings.mqtt_topic, qos=1)
                    if connected_once:
                        self.metrics.mqtt_reconnects += 1
                    connected_once = True
                    delay = self.settings.retry_initial_seconds
                    log_event(
                        self.logger,
                        "mqtt_connected",
                        host=self.settings.mqtt_host,
                        topic=self.settings.mqtt_topic,
                    )
                    await self._consume_connection(client, stop_event)
            except asyncio.CancelledError:
                raise
            except aiomqtt.MqttError as exc:
                log_event(
                    self.logger,
                    "mqtt_connection_failed",
                    level=logging.ERROR,
                    retry_in_seconds=delay,
                    error=str(exc),
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                except TimeoutError:
                    pass
                delay = min(delay * 2, self.settings.retry_max_seconds)

    async def _consume_connection(
        self, client: Any, stop_event: asyncio.Event
    ) -> None:
        async def consume() -> None:
            async for message in client.messages:
                self.metrics.received += 1
                parsed = parse_message(
                    message.payload,
                    str(message.topic),
                    self.settings.max_payload_bytes,
                )
                if isinstance(parsed, ParseFailure):
                    self.metrics.record_discard(parsed.reason)
                    log_event(
                        self.logger,
                        "mqtt_message_discarded",
                        level=logging.DEBUG,
                        topic=str(message.topic),
                        reason=parsed.reason,
                    )
                    continue
                await self.queue.put(parsed)

        consume_task = asyncio.create_task(consume())
        stop_task = asyncio.create_task(stop_event.wait())
        done, pending = await asyncio.wait(
            {consume_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if consume_task in done:
            await consume_task
