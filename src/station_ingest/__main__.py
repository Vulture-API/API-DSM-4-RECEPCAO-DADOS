from __future__ import annotations

import asyncio
import sys

from station_ingest.config import Settings
from station_ingest.logging_config import configure_logging
from station_ingest.service import run_persister, run_service

USAGE = "uso: python -m station_ingest [ingest|persist]"


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    command = args[0] if args else "ingest"
    if command not in ("ingest", "persist"):
        raise SystemExit(USAGE)

    settings = Settings()
    configure_logging(settings.log_level)
    if command == "persist":
        asyncio.run(run_persister(settings))
    else:
        asyncio.run(run_service(settings))


if __name__ == "__main__":
    main()
