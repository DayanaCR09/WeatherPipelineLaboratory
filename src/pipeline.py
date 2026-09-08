"""Weather data pipeline: fetch, transform, and export."""

import asyncio
import logging
import os
from pathlib import Path

import httpx
import pandas as pd
from dotenv import load_dotenv

from cities import load_cities

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
LOG_FILE = PROJECT_ROOT / "pipeline.log"

logger = logging.getLogger("weather_pipeline")


def _level_from_env() -> tuple[int, str, bool]:
    """Return (level, name, recognized) from LOG_LEVEL in .env. Unknown names fall back to INFO."""
    load_dotenv(PROJECT_ROOT / ".env", override=True)
    requested = os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO"
    level = logging.getLevelNamesMapping().get(requested)
    if level is None:
        return logging.INFO, requested, False
    return level, requested, True


def configure_logging() -> None:
    """Write execution details to pipeline.log (and stderr) at the LOG_LEVEL from .env."""
    level, requested, known = _level_from_env()

    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    if not known:
        logger.error("Unknown LOG_LEVEL %r in .env; falling back to INFO", requested)
    else:
        logger.info("Logging at %s to %s", requested, LOG_FILE)


async def fetch(client: httpx.AsyncClient) -> dict:
    """Retrieve raw weather data from the source API."""
    raise NotImplementedError


def transform(payload: dict) -> pd.DataFrame:
    """Normalize the raw payload into a tabular form."""
    raise NotImplementedError


def export(df: pd.DataFrame, path: Path) -> None:
    """Write the transformed data to an Excel report."""
    df.to_excel(path, index=False)


async def main() -> None:
    configure_logging()
    logger.info("Pipeline run started")
    try:
        cities = load_cities()
    except Exception:
        logger.exception("Pipeline aborted while loading city data")
        raise
    logger.info("Pipeline run finished with %d cities ready to fetch", len(cities))


if __name__ == "__main__":
    asyncio.run(main())
