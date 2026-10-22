import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from station_ingest import redis_writer
from station_ingest.config import Settings
from station_ingest.retention import RetentionResult, enforce_once


@pytest.mark.parametrize(
    "kwargs",
    [
        {"retention_required_groups": []},
        {"retention_required_groups": [""]},
        {"retention_required_groups": ["bad\x00name"]},
        {"retention_trim_limit": 0},
        {"retention_max_groups": 0},
    ],
)
def test_invalid_safe_retention_configuration(kwargs):
    with pytest.raises(ValueError):
        Settings(_env_file=None, **kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"required_groups": []},
        {"trim_limit": 0},
        {"max_groups": 0},
    ],
)
async def test_atomic_retention_rejects_invalid_limits_before_redis(kwargs):
    redis = AsyncMock()
    with pytest.raises(ValueError):
        await enforce_once(redis, "stream", 60, **kwargs)
    redis.eval.assert_not_called()


@pytest.mark.parametrize("failure", [False, True])
async def test_retention_loop_reports_blocked_or_failed_cleanup(
    monkeypatch, caplog, failure
):
    stop = asyncio.Event()

    async def enforce(*args, **kwargs):
        stop.set()
        if failure:
            raise ConnectionError("PRIVATE_CONNECTION_DETAILS")
        return RetentionResult(0, "0-0", "required_group_missing", "essential")

    monkeypatch.setattr(redis_writer, "enforce_once", enforce)
    with caplog.at_level(logging.INFO):
        await redis_writer.run_retention(
            AsyncMock(), "stream", 60, 0.01, stop, logging.getLogger("retention")
        )
    assert (
        "redis_retention_failed" if failure else "redis_retention_status"
    ) in caplog.text
    assert "PRIVATE_CONNECTION_DETAILS" not in caplog.text
