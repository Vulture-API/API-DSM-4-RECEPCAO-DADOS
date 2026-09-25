"""Consumidor do Redis Stream que grava as leituras no PostgreSQL.

Fecha o fluxo  estação -> MQTT -> Redis -> PostgreSQL:

- cada entrada do stream vira uma linha em ``readings`` por medição;
- ``stations.last_communication_at`` é atualizado, o que alimenta o status
  Online/Offline da estação (SCRUM-381);
- o motor de regras (API-DSM-4-ALERTAS) lê ``readings`` e dispara os alertas.

Mapeamento do payload:

- ``estacao_id`` é o MAC da estação (``stations.mac_address``). Aceita
  ``AA:BB:CC:DD:EE:FF``, ``aa-bb-cc-dd-ee-ff`` ou ``AABBCCDDEEFF``. Um valor
  só com dígitos é tratado como ``stations.id``.
- toda outra chave numérica é o ``local_identifier`` de um sensor daquela
  estação. Chaves sem sensor cadastrado são ignoradas e contadas.
- ``unix_time`` ausente usa o instante de recepção.

A entrega é "pelo menos uma vez": a entrada só recebe XACK depois do COMMIT.
Se o processo cair entre o COMMIT e o XACK, o lote é regravado ao reiniciar.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from station_ingest.logging_config import log_event

_MAC_PATTERN = re.compile(r"^[0-9A-F]{2}([:-]?)(?:[0-9A-F]{2}\1){4}[0-9A-F]{2}$")
_RESERVED_KEYS = frozenset({"estacao_id", "unix_time"})
# readings.value é decimal(10,2): acima disso o INSERT inteiro falharia.
_MAX_ABS_VALUE = 99_999_999.99


def normalize_station_key(raw: str) -> tuple[str, str] | None:
    """Devolve ("mac", "AA:BB:...") ou ("id", "42"); None se não reconhecer."""
    value = raw.strip().upper()
    if value.isdigit():
        return ("id", str(int(value)))
    if not _MAC_PATTERN.match(value):
        return None
    hex_only = value.replace(":", "").replace("-", "")
    return ("mac", ":".join(hex_only[i : i + 2] for i in range(0, 12, 2)))


@dataclass(frozen=True, slots=True)
class Measurement:
    local_identifier: str
    value: float


@dataclass(frozen=True, slots=True)
class StreamEntry:
    entry_id: str
    station_key: tuple[str, str] | None
    unix_time: int
    measurements: tuple[Measurement, ...]


def _to_int(value: str | None) -> int | None:
    try:
        return int(value) if value not in (None, "", "null") else None
    except (TypeError, ValueError):
        return None


def parse_stream_entry(entry_id: str, fields: dict[str, str]) -> StreamEntry:
    """Nunca lança: entrada malformada vira StreamEntry sem estação/medições,
    que é contada como ignorada e recebe XACK. Uma "poison message" não pode
    travar o consumidor (ela ficaria pendente e derrubaria cada reinício)."""
    received_at = _to_int(fields.get("received_at"))
    unix_time = _to_int(fields.get("unix_time"))
    valid_time = unix_time is not None or received_at is not None
    if unix_time is None:
        unix_time = (received_at or 0) // 1000

    try:
        payload = json.loads(fields.get("payload") or "{}")
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict) or not valid_time:
        return StreamEntry(entry_id, None, unix_time, ())

    measurements: list[Measurement] = []
    for key, value in payload.items():
        if key in _RESERVED_KEYS or isinstance(value, bool):
            continue
        if not isinstance(value, int | float):
            continue
        number = float(value)
        if not math.isfinite(number) or abs(number) > _MAX_ABS_VALUE:
            continue
        measurements.append(Measurement(str(key), number))

    return StreamEntry(
        entry_id=entry_id,
        station_key=normalize_station_key(fields.get("estacao_id", "")),
        unix_time=unix_time,
        measurements=tuple(measurements),
    )


@dataclass(slots=True)
class PersistResult:
    readings: int = 0
    stations_updated: int = 0
    skipped: Counter[str] = field(default_factory=Counter)


class ReadingStore(Protocol):
    async def persist(self, entries: Sequence[StreamEntry]) -> PersistResult: ...


class PgReadingStore:
    """Grava um lote inteiro numa transação só."""

    def __init__(self, pool: Any) -> None:
        self.pool = pool

    async def persist(self, entries: Sequence[StreamEntry]) -> PersistResult:
        result = PersistResult()
        macs = sorted(
            {e.station_key[1] for e in entries if e.station_key and e.station_key[0] == "mac"}
        )
        ids = sorted(
            {int(e.station_key[1]) for e in entries if e.station_key and e.station_key[0] == "id"}
        )

        async with self.pool.acquire() as conn, conn.transaction():
            station_rows = await conn.fetch(
                "SELECT id, mac_address FROM stations "
                "WHERE upper(mac_address) = ANY($1::text[]) OR id = ANY($2::int[])",
                macs,
                ids,
            )
            by_mac = {str(r["mac_address"]).upper(): r["id"] for r in station_rows}
            by_id = {str(r["id"]): r["id"] for r in station_rows}

            station_ids = sorted({r["id"] for r in station_rows})
            sensor_rows = await conn.fetch(
                "SELECT id, station_id, local_identifier FROM sensors "
                "WHERE station_id = ANY($1::int[])",
                station_ids,
            )
            sensors = {
                (r["station_id"], str(r["local_identifier"]).lower()): r["id"]
                for r in sensor_rows
            }

            rows: list[tuple[int, float, int]] = []
            last_seen: dict[int, int] = {}
            for entry in entries:
                if entry.station_key is None:
                    result.skipped["invalid_station_id"] += 1
                    continue
                kind, key = entry.station_key
                station_id = by_mac.get(key) if kind == "mac" else by_id.get(key)
                if station_id is None:
                    result.skipped["unknown_station"] += 1
                    continue

                last_seen[station_id] = max(
                    last_seen.get(station_id, 0), entry.unix_time
                )
                for m in entry.measurements:
                    sensor_id = sensors.get(
                        (station_id, m.local_identifier.lower())
                    )
                    if sensor_id is None:
                        result.skipped["unknown_sensor"] += 1
                        continue
                    rows.append((sensor_id, m.value, entry.unix_time))

            if rows:
                # Entrega "pelo menos uma vez": um lote regravado após queda
                # entre COMMIT e XACK não pode duplicar leituras. O NOT EXISTS
                # usa o índice (sensor_id, unix_time DESC).
                await conn.executemany(
                    "INSERT INTO readings (sensor_id, value, unix_time) "
                    "SELECT $1::int, $2::float8, $3::bigint "
                    "WHERE NOT EXISTS (SELECT 1 FROM readings "
                    "WHERE sensor_id = $1::int AND unix_time = $3::bigint)",
                    rows,
                )
            for station_id, unix_time in last_seen.items():
                await conn.execute(
                    "UPDATE stations SET last_communication_at = "
                    "GREATEST(COALESCE(last_communication_at, 'epoch'), "
                    "to_timestamp($2) AT TIME ZONE 'UTC') WHERE id = $1",
                    station_id,
                    unix_time,
                )

        result.readings = len(rows)
        result.stations_updated = len(last_seen)
        return result


class StreamClient(Protocol):
    async def xgroup_create(
        self, name: str, groupname: str, id: str, mkstream: bool
    ) -> Any: ...

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int,
        block: int | None,
    ) -> Any: ...

    async def xack(self, name: str, groupname: str, *ids: str) -> int: ...


class StreamPersister:
    def __init__(
        self,
        redis: StreamClient,
        store: ReadingStore,
        *,
        stream: str,
        group: str,
        consumer: str,
        batch_size: int,
        block_ms: int,
        retry_initial_seconds: float,
        retry_max_seconds: float,
        logger: logging.Logger,
    ) -> None:
        self.redis = redis
        self.store = store
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.batch_size = batch_size
        self.block_ms = block_ms
        self.retry_initial_seconds = retry_initial_seconds
        self.retry_max_seconds = retry_max_seconds
        self.logger = logger
        self.readings_written = 0

    async def ensure_group(self) -> None:
        try:
            # "0": um grupo novo também processa o que já está no stream.
            await self.redis.xgroup_create(
                self.stream, self.group, id="0", mkstream=True
            )
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read_batch(self, pending: bool) -> list[tuple[str, dict[str, str]]]:
        response = await self.redis.xreadgroup(
            self.group,
            self.consumer,
            {self.stream: "0" if pending else ">"},
            count=self.batch_size,
            block=None if pending else self.block_ms,
        )
        if not response:
            return []
        _, messages = response[0]
        # Entradas já expurgadas pelo XTRIM voltam sem dados; entram no lote
        # para receberem XACK e não ficarem pendentes para sempre.
        return [(entry_id, data or {}) for entry_id, data in messages]

    async def process_once(self, pending: bool = False) -> int:
        messages = await self.read_batch(pending)
        if not messages:
            return 0
        entries = [parse_stream_entry(i, f) for i, f in messages]
        result = await self._persist_with_retry(entries)
        await self.redis.xack(self.stream, self.group, *[i for i, _ in messages])
        self.readings_written += result.readings
        log_event(
            self.logger,
            "readings_persisted",
            entries=len(entries),
            readings=result.readings,
            stations_updated=result.stations_updated,
            **{f"skipped_{k}": v for k, v in result.skipped.items()},
        )
        return len(messages)

    async def run(self, stop_event: asyncio.Event) -> None:
        await self.ensure_group()
        # Primeiro o que ficou pendente (lido e não confirmado antes de uma queda).
        pending = True
        while not stop_event.is_set():
            try:
                processed = await self.process_once(pending=pending)
                if pending and not processed:
                    pending = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log_event(
                    self.logger,
                    "persister_read_failed",
                    level=logging.ERROR,
                    pending=pending,
                    error=str(exc),
                )
                await asyncio.sleep(self.retry_initial_seconds)

    async def _persist_with_retry(
        self, entries: Sequence[StreamEntry]
    ) -> PersistResult:
        delay = self.retry_initial_seconds
        while True:
            try:
                return await self.store.persist(entries)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log_event(
                    self.logger,
                    "postgres_batch_failed",
                    level=logging.ERROR,
                    batch_size=len(entries),
                    retry_in_seconds=delay,
                    error=str(exc),
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.retry_max_seconds)
