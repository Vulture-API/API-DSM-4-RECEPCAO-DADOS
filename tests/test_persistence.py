import asyncio
import json
import logging
import os
from collections.abc import Sequence

import pytest

from station_ingest.persistence import (
    Measurement,
    PersistResult,
    StreamEntry,
    StreamPersister,
    normalize_station_key,
    parse_stream_entry,
)


def fields(payload: dict, unix_time: str = "1760000000") -> dict[str, str]:
    return {
        "estacao_id": str(payload.get("estacao_id", "")),
        "unix_time": unix_time,
        "received_at": "1760000005000",
        "topic": "estacoes/x/dados",
        "payload": json.dumps(payload),
    }


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("AA:BB:CC:DD:EE:01", ("mac", "AA:BB:CC:DD:EE:01")),
        ("aa-bb-cc-dd-ee-01", ("mac", "AA:BB:CC:DD:EE:01")),
        ("aabbccddee01", ("mac", "AA:BB:CC:DD:EE:01")),
        (" 42 ", ("id", "42")),
        ("aa:bb-cc:dd:ee:01", None),
        ("estacao-42", None),
        ("", None),
    ],
)
def test_normalize_station_key(raw, expected):
    assert normalize_station_key(raw) == expected


def test_parse_keeps_only_finite_numbers():
    entry = parse_stream_entry(
        "1-0",
        fields(
            {
                "estacao_id": "AA:BB:CC:DD:EE:01",
                "unix_time": 1760000000,
                "temp": 23.5,
                "umid": 60,
                "ligado": True,
                "texto": "x",
                "enorme": 1e12,
            }
        ),
    )
    assert entry.station_key == ("mac", "AA:BB:CC:DD:EE:01")
    assert entry.unix_time == 1760000000
    assert entry.measurements == (
        Measurement("temp", 23.5),
        Measurement("umid", 60.0),
    )


def test_parse_uses_received_at_without_unix_time():
    entry = parse_stream_entry("1-0", fields({"estacao_id": "1"}, "null"))
    assert entry.unix_time == 1760000005


def test_parse_tolerates_bad_payload():
    entry = parse_stream_entry(
        "1-0", {"estacao_id": "1", "payload": "{nope", "received_at": "0"}
    )
    assert entry.measurements == ()


class FakeStore:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.calls: list[Sequence[StreamEntry]] = []

    async def persist(self, entries):
        if self.failures:
            self.failures -= 1
            raise ConnectionError("pg down")
        self.calls.append(entries)
        return PersistResult(readings=sum(len(e.measurements) for e in entries))


class FakeRedis:
    def __init__(self, new=None, pending=None, group_exists=False) -> None:
        self.new = list(new or [])
        self.pending = list(pending or [])
        self.group_exists = group_exists
        self.acked: list[str] = []

    async def xgroup_create(self, name, groupname, id, mkstream):
        if self.group_exists:
            raise Exception("BUSYGROUP Consumer Group name already exists")
        assert (id, mkstream) == ("0", True)

    async def xreadgroup(self, groupname, consumername, streams, count, block):
        ((stream, offset),) = streams.items()
        source = self.pending if offset == "0" else self.new
        batch, source[:] = source[:count], source[count:]
        if not batch and block:
            await asyncio.sleep(block / 1000)  # como o BLOCK do Redis
        return [(stream, batch)] if batch else []

    async def xack(self, name, groupname, *ids):
        self.acked.extend(ids)
        return len(ids)


def make(redis, store, batch_size=10):
    return StreamPersister(
        redis,
        store,
        stream="s",
        group="g",
        consumer="c",
        batch_size=batch_size,
        block_ms=10,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.002,
        logger=logging.getLogger("test"),
    )


async def test_process_once_persists_and_acks():
    redis = FakeRedis(new=[("1-0", fields({"estacao_id": "1", "t": 1}))])
    store = FakeStore()
    persister = make(redis, store)
    assert await persister.process_once() == 1
    assert redis.acked == ["1-0"]
    assert persister.readings_written == 1


async def test_retries_postgres_before_ack():
    redis = FakeRedis(new=[("1-0", fields({"estacao_id": "1", "t": 1}))])
    store = FakeStore(failures=2)
    await make(redis, store).process_once()
    assert len(store.calls) == 1
    assert redis.acked == ["1-0"]


async def test_trimmed_pending_entries_are_acked():
    redis = FakeRedis(pending=[("1-0", None)])
    store = FakeStore()
    assert await make(redis, store).process_once(pending=True) == 1
    assert redis.acked == ["1-0"]


async def test_run_drains_pending_then_new_and_stops():
    redis = FakeRedis(
        pending=[("1-0", fields({"estacao_id": "1", "t": 1}))],
        new=[("2-0", fields({"estacao_id": "1", "t": 2}))],
        group_exists=True,
    )
    store = FakeStore()
    stop = asyncio.Event()
    persister = make(redis, store)
    task = asyncio.create_task(persister.run(stop))
    for _ in range(100):
        if len(redis.acked) == 2:
            break
        await asyncio.sleep(0.005)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert redis.acked == ["1-0", "2-0"]


async def test_ensure_group_propagates_other_errors():
    class Broken(FakeRedis):
        async def xgroup_create(self, *a, **k):
            raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        await make(Broken(), FakeStore()).ensure_group()


DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


@pytest.mark.integration
@pytest.mark.skipif(not DATABASE_URL, reason="set TEST_DATABASE_URL")
async def test_pg_store_writes_readings_and_last_communication():
    import asyncpg

    from station_ingest.persistence import PgReadingStore

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TEMP TABLE stations (id int primary key, mac_address text,
                  last_communication_at timestamp);
                CREATE TEMP TABLE sensors (id int primary key, station_id int,
                  local_identifier text);
                CREATE TEMP TABLE readings (id serial, sensor_id int,
                  value numeric(10,2), unix_time bigint);
                INSERT INTO stations VALUES (7, 'AA:BB:CC:DD:EE:07', NULL);
                INSERT INTO sensors VALUES (70, 7, 'TEMP'), (71, 7, 'umid');
                """
            )
            store = PgReadingStore(_SingleConn(conn))
            entries = [
                parse_stream_entry(
                    "1-0",
                    fields(
                        {"estacao_id": "aa-bb-cc-dd-ee-07", "temp": 30.5,
                         "umid": 70, "vento": 3},
                        "1760000000",
                    ),
                ),
                parse_stream_entry("2-0", fields({"estacao_id": "FF:FF:FF:FF:FF:FF", "temp": 1})),
            ]
            result = await store.persist(entries)
            assert result.readings == 2
            # Reentrega do mesmo lote (queda entre COMMIT e XACK) não duplica.
            await store.persist(entries)
            assert await conn.fetchval("SELECT count(*) FROM readings") == 2
            assert result.skipped == {"unknown_sensor": 1, "unknown_station": 1}
            rows = await conn.fetch("SELECT sensor_id, value FROM readings ORDER BY sensor_id")
            assert [(r[0], float(r[1])) for r in rows] == [(70, 30.5), (71, 70.0)]
            last = await conn.fetchval("SELECT last_communication_at FROM stations")
            assert last.isoformat() == "2025-10-09T08:53:20"
    finally:
        await pool.close()


class _SingleConn:
    """Usa sempre a mesma conexão, para as tabelas TEMP ficarem visíveis."""

    def __init__(self, conn) -> None:
        self.conn = conn

    def acquire(self):
        conn = self.conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


def test_main_rejects_unknown_command():
    from station_ingest.__main__ import main

    with pytest.raises(SystemExit, match="ingest|persist"):
        main(["xyz"])


def test_parse_never_raises_on_garbage_fields():
    entry = parse_stream_entry(
        "1-0",
        {"estacao_id": "1", "unix_time": "abc", "received_at": "xyz", "payload": "[1, 2]"},
    )
    assert entry.station_key is None
    assert entry.measurements == ()


async def test_poison_pending_entry_is_acked_and_does_not_stop_the_consumer():
    redis = FakeRedis(
        pending=[("1-0", {"estacao_id": "1", "unix_time": "abc", "payload": "{}"})],
        new=[("2-0", fields({"estacao_id": "1", "t": 2}))],
    )
    store = FakeStore()
    stop = asyncio.Event()
    task = asyncio.create_task(make(redis, store).run(stop))
    for _ in range(100):
        if len(redis.acked) == 2:
            break
        await asyncio.sleep(0.005)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert redis.acked == ["1-0", "2-0"]


async def test_pending_loop_survives_store_errors_until_it_recovers():
    redis = FakeRedis(pending=[("1-0", fields({"estacao_id": "1", "t": 1}))])

    class FlakyRead(FakeRedis):
        calls = 0

        async def xreadgroup(self, *args, **kwargs):
            FlakyRead.calls += 1
            if FlakyRead.calls == 1:
                raise ConnectionError("redis caiu")
            return await super().xreadgroup(*args, **kwargs)

    flaky = FlakyRead(pending=redis.pending)
    stop = asyncio.Event()
    task = asyncio.create_task(make(flaky, FakeStore()).run(stop))
    for _ in range(100):
        if flaky.acked:
            break
        await asyncio.sleep(0.005)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert flaky.acked == ["1-0"]
