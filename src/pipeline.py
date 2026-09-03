"""Weather data pipeline: fetch, transform, and export."""

import asyncio
import logging
import os
import re
import unicodedata
from pathlib import Path

import httpx
import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
RAW_CITIES = DATA_DIR / "raw_cities.csv"
LOG_FILE = PROJECT_ROOT / "pipeline.log"

REQUIRED_COLUMNS = ("city_name", "country", "latitude", "longitude")
LATITUDE_RANGE = (-90.0, 90.0)
LONGITUDE_RANGE = (-180.0, 180.0)

# Spellings that title-casing alone cannot repair.
COUNTRY_ALIASES = {
    "us": "United States",
    "usa": "United States",
    "uk": "United Kingdom",
}

_ALIAS_IN_PARENS = re.compile(r"\s*\([^)]*\)")
_NOISE = re.compile(r"[\d_]|[^\w\s'\-]")
_WHITESPACE = re.compile(r"\s+")
_WORD = re.compile(r"[^\W\d_]+")

logger = logging.getLogger("weather_pipeline")


def configure_logging() -> None:
    """Log to pipeline.log and the console at the level named in .env."""
    load_dotenv()
    requested = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = logging.getLevelNamesMapping().get(requested)

    logging.basicConfig(
        level=level or logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )

    if level is None:
        logger.error("Unknown LOG_LEVEL %r in .env; falling back to INFO", requested)
    else:
        logger.info("Logging at %s to %s", requested, LOG_FILE)


def _title_case(value: str) -> str:
    """Capitalize each word, leaving accents and non-Latin scripts intact."""
    return _WORD.sub(lambda match: match.group().capitalize(), value)


def normalize_city(raw: str) -> str:
    """Reduce a messy city field to a single title-cased name."""
    name = unicodedata.normalize("NFC", str(raw))
    name = name.split(",")[0]  # "mexico city, cdmx"
    name = name.split("/")[0]  # "bei jing / 北京"
    name = _ALIAS_IN_PARENS.sub("", name)  # "mumbai (bombay)"
    name = _NOISE.sub("", name)  # "sydney***", "seoul#1"
    name = _WHITESPACE.sub(" ", name).strip(" -'")
    return _title_case(name)


def normalize_country(raw: str) -> str:
    """Collapse spacing and casing, expanding known abbreviations."""
    country = unicodedata.normalize("NFC", str(raw))
    country = _WHITESPACE.sub(" ", country).strip()
    alias = COUNTRY_ALIASES.get(country.casefold().replace(".", ""))
    if alias:
        return alias
    return _title_case(_NOISE.sub("", country)).strip()


def normalize_column(name: str) -> str:
    """Turn a header such as ' LONGITUDE' into 'longitude'."""
    return re.sub(r"\W+", "_", name.strip()).strip("_").lower()


def _parse_coordinate(value: str, field: str, bounds: tuple[float, float]) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field} is not numeric ({value!r})") from error
    low, high = bounds
    if not low <= number <= high:
        raise ValueError(f"{field} {number} is outside [{low}, {high}]")
    return number


def load_cities(path: Path = RAW_CITIES) -> pd.DataFrame:
    """Read the raw city CSV and return a normalized, validated DataFrame."""
    logger.info("Reading raw city data from %s", path)
    try:
        frame = pd.read_csv(path, dtype=str, skipinitialspace=True)
    except FileNotFoundError:
        logger.error("Raw city file is missing: %s", path)
        raise
    except pd.errors.ParserError:
        logger.error("Raw city file is malformed and could not be parsed: %s", path)
        raise

    frame.columns = [normalize_column(column) for column in frame.columns]
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        logger.error("Missing required columns: %s", ", ".join(missing))
        raise KeyError(f"Missing required columns: {', '.join(missing)}")

    logger.info("Read %d raw rows", len(frame))

    records = []
    for line, row in enumerate(frame.to_dict("records"), start=2):
        city = normalize_city(row["city_name"])
        if not city:
            logger.error(
                "Line %d: city name is empty after normalization (%r)",
                line,
                row["city_name"],
            )
            continue
        try:
            latitude = _parse_coordinate(row["latitude"], "latitude", LATITUDE_RANGE)
            longitude = _parse_coordinate(
                row["longitude"], "longitude", LONGITUDE_RANGE
            )
        except ValueError as error:
            logger.error("Line %d (%s): %s", line, city, error)
            continue

        logger.debug("Line %d: %r -> %r", line, row["city_name"], city)
        records.append(
            {
                "city_name": city,
                "country": normalize_country(row["country"]),
                "latitude": latitude,
                "longitude": longitude,
            }
        )

    cleaned = pd.DataFrame.from_records(records, columns=list(REQUIRED_COLUMNS))
    duplicates = cleaned.duplicated(subset=["city_name", "country"])
    if duplicates.any():
        logger.warning("Discarding %d duplicate cities", int(duplicates.sum()))
        cleaned = cleaned[~duplicates].reset_index(drop=True)

    rejected = len(frame) - len(cleaned)
    if rejected:
        logger.warning("Rejected %d of %d rows", rejected, len(frame))
    logger.info("Normalized %d cities", len(cleaned))
    return cleaned


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
