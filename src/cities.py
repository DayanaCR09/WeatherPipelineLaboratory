"""Parse raw_cities.csv and normalize city names with regex and string cleanup."""

from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path

import pandas as pd

logger = logging.getLogger("weather_pipeline")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_CITIES = PROJECT_ROOT / "data" / "raw_cities.csv"

REQUIRED_COLUMNS = ("city_name", "country", "latitude", "longitude")
LATITUDE_RANGE = (-90.0, 90.0)
LONGITUDE_RANGE = (-180.0, 180.0)

# Spellings that title-casing alone cannot repair.
COUNTRY_ALIASES = {
    "us": "United States",
    "usa": "United States",
    "uk": "United Kingdom",
}
CITY_ALIASES = {
    "bei jing": "Beijing",
}

_PRIMARY_NAME = re.compile(r"^[^,/]+")
_ALIAS_IN_PARENS = re.compile(r"\s*\([^)]*\)")
_NOISE = re.compile(r"[\d_]|[^\w\s'\-]")
_WHITESPACE = re.compile(r"\s+")
_WORD = re.compile(r"[^\W\d_]+")
_HEADER_NOISE = re.compile(r"\W+")


def _title_case(value: str) -> str:
    """Capitalize each word, leaving accents and non-Latin scripts intact."""
    return _WORD.sub(lambda match: match.group().capitalize(), value)


def normalize_city(raw: str) -> str:
    """Reduce a messy city field to a single title-cased name.

    Uses regex search/replace plus strip and title-case so punctuation, aliases,
    extra spacing, and scrambled casing all collapse to one canonical name.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""

    name = unicodedata.normalize("NFC", str(raw))
    primary = _PRIMARY_NAME.search(name)
    if primary:
        name = primary.group().strip()
    if _ALIAS_IN_PARENS.search(name):
        name = _ALIAS_IN_PARENS.sub("", name)
    name = _NOISE.sub("", name)
    name = _WHITESPACE.sub(" ", name).strip(" -'")
    alias = CITY_ALIASES.get(name.casefold())
    if alias:
        return alias
    return _title_case(name)


def normalize_country(raw: str) -> str:
    """Collapse spacing and casing, expanding known abbreviations."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""

    country = unicodedata.normalize("NFC", str(raw))
    country = _WHITESPACE.sub(" ", country).strip()
    alias = COUNTRY_ALIASES.get(country.casefold().replace(".", ""))
    if alias:
        return alias
    return _title_case(_NOISE.sub("", country)).strip()


def normalize_column(name: str) -> str:
    """Turn a header such as ' LONGITUDE' into 'longitude'."""
    return _HEADER_NOISE.sub("_", name.strip()).strip("_").lower()


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
