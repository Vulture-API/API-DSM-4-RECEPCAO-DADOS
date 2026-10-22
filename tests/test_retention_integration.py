"""Redis 7 atomic trimming protects every group's not-yet-completed entries."""

import asyncio
import os
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from station_ingest.retention import enforce_once

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1", reason="Disposable Redis required"
    ),
]


@pytest.fixture
async def stream():
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=True)
    name = f"retention:{uuid4().hex}"
    # Small listpacks make approximate trim deterministic while preserving its safety semantics.
    await redis.config_set("stream-node-max-entries", 10)
    try:
        for i in range(1, 101):
            await redis.xadd(name, {"payload": str(i)}, id=f"{i}-0")
        yield redis, name
    finally:
        await redis.delete(name)
        await redis.config_set("stream-node-max-entries", 100)
        await redis.aclose()


async def remaining(redis, name):
    return [identifier for identifier, _ in await redis.xrange(name)]


async def test_real_retention_fails_closed_without_required_groups(stream):
    redis, name = stream
    assert (await enforce_once(redis, name, 1)).reason == "no_groups"
    await redis.xgroup_create(name, "other", id="100-0")
    assert (await enforce_once(redis, name, 1)).reason == "required_group_missing"
    assert len(await remaining(redis, name)) == 100
    assert (
        await enforce_once(redis, name, 1, max_groups=1, required_groups=["other"])
    ).removed > 0


async def test_real_retention_never_removes_pending_or_undelivered(stream):
    redis, name = stream
    await redis.xgroup_create(name, "postgres-persister", id="0-0")
    before = await enforce_once(redis, name, 1)
    assert before.boundary == "0-0" and before.removed == 0
    messages = await redis.xreadgroup("postgres-persister", "c", {name: ">"}, count=40)
    ids = [i for i, _ in messages[0][1]]
    await redis.xack(name, "postgres-persister", *ids[:20])
    result = await enforce_once(redis, name, 1)
    assert result.boundary == "21-0" and result.reason == "pending"
    left = await remaining(redis, name)
    assert all(f"{i}-0" in left for i in range(21, 101))
    assert len(left) < 100
    # Even if the data is old, no reader progress means no age-only deletion.
    assert (await redis.xpending(name, "postgres-persister"))["pending"] == 20
    await redis.xack(name, "postgres-persister", *ids[20:])
    assert (await enforce_once(redis, name, 1)).boundary == "40-0"
    left = await remaining(redis, name)
    assert all(f"{i}-0" in left for i in range(40, 101))


async def test_real_retention_honors_all_groups_and_limits(stream):
    redis, name = stream
    await redis.xgroup_create(name, "postgres-persister", id="100-0")
    await redis.xgroup_create(name, "analytics", id="10-0")
    assert (
        await enforce_once(redis, name, 1, max_groups=1)
    ).reason == "too_many_groups"
    result = await enforce_once(redis, name, 1)
    assert result.boundary == "10-0" and result.blocking_group == "analytics"
    left = await remaining(redis, name)
    assert all(f"{i}-0" in left for i in range(10, 101))
    await redis.xgroup_setid(name, "analytics", "100-0")
    result = await enforce_once(redis, name, 1, trim_limit=10)
    assert result.removed <= 10


async def test_real_retention_uses_server_age_and_exact_large_ids(stream):
    redis, name = stream
    now, _ = await redis.time()
    recent = f"{now * 1000}-0"
    await redis.xadd(name, {"payload": "new"}, id=recent)
    await redis.xgroup_create(name, "postgres-persister", id=recent)
    result = await enforce_once(redis, name, 60)
    assert result.reason == "age" and result.removed == 100
    assert recent in await remaining(redis, name)
    # Consecutive sequence numbers above double's exact range must compare as strings.
    huge = "18446744073709551600"
    await redis.xadd(name, {"payload": "large"}, id=f"{huge}-9007199254740992")
    await redis.xadd(name, {"payload": "large2"}, id=f"{huge}-9007199254740993")
    await redis.xgroup_setid(name, "postgres-persister", f"{huge}-9007199254740993")
    # A future ID cannot override the server age boundary.
    assert (await enforce_once(redis, name, 60)).reason == "age"
    assert f"{huge}-9007199254740992" in await remaining(redis, name)


async def test_real_retention_concurrent_delivery_and_ack_preserves_uncompleted(stream):
    redis, name = stream
    await redis.xgroup_create(name, "postgres-persister", id="0-0")

    async def consume():
        for _ in range(5):
            rows = await redis.xreadgroup(
                "postgres-persister", "c", {name: ">"}, count=10
            )
            ids = [identifier for identifier, _ in rows[0][1]]
            await redis.xack(name, "postgres-persister", *ids[:5])

    async def trim():
        for _ in range(8):
            await enforce_once(redis, name, 1)

    await asyncio.gather(consume(), trim())
    left = set(await remaining(redis, name))
    pending = await redis.xpending_range(name, "postgres-persister", "-", "+", 100)
    assert {p["message_id"] for p in pending} <= left
    assert all(f"{i}-0" in left for i in range(51, 101))


async def test_real_retention_compares_large_sequence_components_exactly(stream):
    redis, name = stream
    first = "100-9007199254740992"
    last = "100-9007199254740993"
    await redis.xadd(name, {"payload": "pending"}, id=first)
    await redis.xadd(name, {"payload": "delivered"}, id=last)
    await redis.xgroup_create(name, "postgres-persister", id="100-0")
    await redis.xreadgroup("postgres-persister", "c", {name: ">"}, count=2)
    await redis.xack(name, "postgres-persister", last)
    result = await enforce_once(redis, name, 1)
    assert result.boundary == first and result.reason == "pending"
    assert first in await remaining(redis, name)
