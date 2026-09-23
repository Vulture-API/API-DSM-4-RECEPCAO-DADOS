from __future__ import annotations

import asyncio

from station_ingest.config import Settings
from station_ingest.logging_config import configure_logging
from station_ingest.service import run_service


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    asyncio.run(run_service(settings))


if __name__ == "__main__":
    main()

