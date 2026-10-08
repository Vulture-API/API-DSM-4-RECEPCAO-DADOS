import asyncio
import signal

import pytest

from station_ingest import __main__ as entrypoint
from station_ingest.config import Settings
from station_ingest.service import install_signal_handlers, run_persister


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    called: list[str] = []

    async def fake_service(settings: Settings) -> None:
        called.append("ingest")

    async def fake_persister(settings: Settings) -> None:
        called.append("persist")

    monkeypatch.setattr(entrypoint, "run_service", fake_service)
    monkeypatch.setattr(entrypoint, "run_persister", fake_persister)
    monkeypatch.setattr(entrypoint, "configure_logging", lambda level: None)
    return called


def test_main_runs_ingest_by_default(calls: list[str]) -> None:
    entrypoint.main([])

    assert calls == ["ingest"]


def test_main_runs_persist(calls: list[str]) -> None:
    entrypoint.main(["persist"])

    assert calls == ["persist"]


def test_main_rejects_unknown_command(calls: list[str]) -> None:
    with pytest.raises(SystemExit, match="uso:"):
        entrypoint.main(["other"])

    assert calls == []


@pytest.mark.asyncio
async def test_signal_handler_sets_stop_event() -> None:
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event, signals=(signal.SIGUSR1,))
    try:
        signal.raise_signal(signal.SIGUSR1)
        await asyncio.wait_for(stop_event.wait(), timeout=1)
    finally:
        asyncio.get_running_loop().remove_signal_handler(signal.SIGUSR1)

    assert stop_event.is_set()


@pytest.mark.asyncio
async def test_persister_requires_database_url() -> None:
    settings = Settings(_env_file=None, database_url=None)

    with pytest.raises(RuntimeError, match="INGEST_DATABASE_URL"):
        await run_persister(settings)
