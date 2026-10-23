from __future__ import annotations

import asyncio
import sys

from station_ingest.config import Settings
from station_ingest.logging_config import configure_logging
from station_ingest.service import run_service

USAGE = "uso: python -m station_ingest [ingest]"


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    command = args[0] if args else "ingest"
    if command == "persist":
        raise SystemExit(
            "O modo persist foi removido da recepção. "
            "Use station-persist consume no serviço API-DSM-4-PERSISTENCIA-DADOS."
        )
    if command != "ingest" or len(args) > 1:
        raise SystemExit(USAGE)

    settings = Settings()
    configure_logging(settings.log_level)
    asyncio.run(run_service(settings))


if __name__ == "__main__":
    main()
