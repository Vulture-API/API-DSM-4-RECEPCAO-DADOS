import asyncio
import signal

import pytest

from station_ingest import __main__ as entrypoint
from station_ingest.config import Settings
from station_ingest.service import install_signal_handlers


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, object]]:
    called: list[tuple[str, object]] = []
    settings = Settings(_env_file=None, log_level="DEBUG")

    async def fake_service(received: Settings) -> None:
        called.append(("ingest", received))

    monkeypatch.setattr(entrypoint, "Settings", lambda: settings)
    monkeypatch.setattr(entrypoint, "run_service", fake_service)
    monkeypatch.setattr(
        entrypoint, "configure_logging", lambda level: called.append(("log", level))
    )
    return called


def test_main_runs_ingest_by_default(calls: list[tuple[str, object]]) -> None:
    entrypoint.main([])

    assert [name for name, _ in calls] == ["log", "ingest"]
    assert calls[0][1] == "DEBUG"
    assert isinstance(calls[1][1], Settings)


def test_main_runs_explicit_ingest(calls: list[tuple[str, object]]) -> None:
    entrypoint.main(["ingest"])

    assert [name for name, _ in calls] == ["log", "ingest"]


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


@pytest.mark.parametrize("args", [["persist"], ["persist", "extra"]])
def test_main_rejects_removed_persist_before_configuration(monkeypatch, args):
    def unexpected():
        pytest.fail("Removed command must not configure or connect services")

    monkeypatch.setattr(entrypoint, "Settings", unexpected)
    with pytest.raises(SystemExit, match="station-persist consume") as exc:
        entrypoint.main(args)
    assert exc.value.code != 0


@pytest.mark.parametrize("args", [["unknown"], ["ingest", "extra"]])
def test_main_rejects_unknown_arguments(args):
    with pytest.raises(SystemExit, match="uso:"):
        entrypoint.main(args)
