"""Weather data pipeline: fetch, transform, and export."""

import asyncio
import logging
import os
import ssl
from pathlib import Path

import httpx
import pandas as pd
import truststore
from dotenv import load_dotenv

from cities import load_cities

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
LOG_FILE = PROJECT_ROOT / "pipeline.log"

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
FORECAST_TIMEOUT = 30.0
FORECAST_CONCURRENCY = 8
CURRENT_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
)
DAILY_VARIABLES = (
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "weather_code",
)
TEMPERATURE_UNITS = frozenset({"celsius", "fahrenheit"})

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


def _temperature_unit() -> str:
    unit = os.getenv("WEATHER_UNIT", "celsius").strip().lower() or "celsius"
    if unit not in TEMPERATURE_UNITS:
        logger.error("Unknown WEATHER_UNIT %r in .env; falling back to celsius", unit)
        return "celsius"
    return unit


def _max_retries() -> int:
    raw = os.getenv("MAX_RETRIES", "3").strip() or "3"
    try:
        value = int(raw)
    except ValueError:
        logger.error("Invalid MAX_RETRIES %r in .env; falling back to 3", raw)
        return 3
    if value < 1:
        logger.error("MAX_RETRIES must be >= 1; falling back to 3")
        return 3
    return value


def _forecast_params(latitude: float, longitude: float) -> dict[str, float | str]:
    return {
        "latitude": latitude,
        "longitude": longitude,
        "current": ",".join(CURRENT_VARIABLES),
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "auto",
        "temperature_unit": _temperature_unit(),
    }


def _is_retryable(error: BaseException) -> bool:
    if isinstance(error, httpx.HTTPStatusError):
        status = error.response.status_code
        return status == 429 or status >= 500
    return isinstance(error, httpx.RequestError)


async def _get_forecast(
    client: httpx.AsyncClient, city: str, params: dict[str, float | str]
) -> dict:
    """GET /v1/forecast, retrying transient failures up to MAX_RETRIES."""
    attempts = _max_retries()
    last_error: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = await client.get(FORECAST_URL, params=params)
            if response.status_code == 400:
                try:
                    reason = response.json().get("reason", response.text)
                except ValueError:
                    reason = response.text
                raise RuntimeError(f"Open-Meteo rejected {city}: {reason}")
            response.raise_for_status()
            payload = response.json()
            if payload.get("error"):
                raise RuntimeError(payload.get("reason", "Open-Meteo returned an error"))
            return payload
        except Exception as error:
            last_error = error
            if not _is_retryable(error):
                logger.error("%s: forecast request failed (%s)", city, error)
                raise
            logger.warning(
                "%s: forecast attempt %d/%d failed (%s)",
                city,
                attempt,
                attempts,
                error,
            )
            if attempt < attempts:
                await asyncio.sleep(min(2 ** (attempt - 1), 8))
    logger.error("%s: giving up after %d attempts", city, attempts)
    raise last_error if last_error else RuntimeError(f"No forecast for {city}")


async def _fetch_city(
    client: httpx.AsyncClient,
    row: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    city = row["city_name"]
    params = _forecast_params(row["latitude"], row["longitude"])
    logger.info(
        "Requesting forecast for %s (%s) at %s, %s",
        city,
        row["country"],
        row["latitude"],
        row["longitude"],
    )
    async with semaphore:
        payload = await _get_forecast(client, city, params)
    current = payload.get("current") or {}
    temperature = current.get("temperature_2m")
    unit = (payload.get("current_units") or {}).get("temperature_2m", "")
    logger.info("%s: current temperature %s %s", city, temperature, unit)
    payload["city_name"] = city
    payload["country"] = row["country"]
    return payload


async def fetch(client: httpx.AsyncClient, cities: pd.DataFrame) -> list[dict]:
    """Retrieve current and daily forecasts from Open-Meteo for each city."""
    if cities.empty:
        logger.error("No cities available to fetch forecasts for")
        raise ValueError("No cities available to fetch forecasts for")

    unit = _temperature_unit()
    logger.info(
        "Fetching Open-Meteo forecasts for %d cities (%s) from %s",
        len(cities),
        unit,
        FORECAST_URL,
    )
    semaphore = asyncio.Semaphore(FORECAST_CONCURRENCY)
    results = await asyncio.gather(
        *(
            _fetch_city(client, row, semaphore)
            for row in cities.to_dict("records")
        ),
        return_exceptions=True,
    )

    payloads: list[dict] = []
    for row, result in zip(cities.to_dict("records"), results, strict=True):
        if isinstance(result, Exception):
            logger.error("Failed to fetch forecast for %s: %s", row["city_name"], result)
            continue
        payloads.append(result)

    logger.info("Fetched %d of %d forecasts", len(payloads), len(cities))
    if not payloads:
        raise RuntimeError("No forecasts were retrieved from Open-Meteo")
    return payloads


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
    try:
        async with httpx.AsyncClient(
            timeout=FORECAST_TIMEOUT,
            headers={"User-Agent": "WeatherPipelineLaboratory/0.1"},
            verify=truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
        ) as client:
            forecasts = await fetch(client, cities)
    except Exception:
        logger.exception("Pipeline aborted while fetching forecasts")
        raise
    logger.info("Pipeline run finished with %d forecasts ready to transform", len(forecasts))


if __name__ == "__main__":
    asyncio.run(main())
