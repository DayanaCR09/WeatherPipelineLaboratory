"""Weather data pipeline: fetch, transform, and export."""

import asyncio
import logging
import os
import ssl
import time
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
HOURLY_VARIABLES = ("temperature_2m", "precipitation")
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
        "hourly": ",".join(HOURLY_VARIABLES),
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


async def _fetch_city(client: httpx.AsyncClient, row: dict) -> dict:
    city = row["city_name"]
    params = _forecast_params(row["latitude"], row["longitude"])
    logger.info(
        "Requesting hourly forecast for %s (%s) at %s, %s",
        city,
        row["country"],
        row["latitude"],
        row["longitude"],
    )
    payload = await _get_forecast(client, city, params)
    hourly = payload.get("hourly") or {}
    hours = hourly.get("time") or []
    logger.info("%s: received %d hourly points", city, len(hours))
    payload["city_name"] = city
    payload["country"] = row["country"]
    return payload


async def fetch(client: httpx.AsyncClient, cities: pd.DataFrame) -> list[dict]:
    """Request hourly forecasts from Open-Meteo one city at a time."""
    if cities.empty:
        logger.error("No cities available to fetch forecasts for")
        raise ValueError("No cities available to fetch forecasts for")

    unit = _temperature_unit()
    logger.info(
        "Fetching Open-Meteo hourly forecasts sequentially for %d cities (%s) from %s",
        len(cities),
        unit,
        FORECAST_URL,
    )
    started = time.perf_counter()
    payloads: list[dict] = []
    for row in cities.to_dict("records"):
        try:
            payloads.append(await _fetch_city(client, row))
        except (httpx.HTTPError, RuntimeError, ValueError, OSError) as error:
            logger.error("Failed to fetch forecast for %s: %s", row["city_name"], error)

    elapsed = time.perf_counter() - started
    logger.info(
        "Sequential fetch of %d cities finished in %.3f seconds (%d succeeded)",
        len(cities),
        elapsed,
        len(payloads),
    )
    if not payloads:
        raise RuntimeError("No forecasts were retrieved from Open-Meteo")
    return payloads


def _pad_series(values: object, length: int) -> list:
    items = list(values or [])
    if len(items) < length:
        items = items + [None] * (length - len(items))
    return items[:length]


def _hourly_frame(payload: dict) -> pd.DataFrame:
    """Parse one Open-Meteo JSON body into an hourly DataFrame."""
    city = payload.get("city_name", "unknown")
    hourly = payload.get("hourly") or {}
    times = list(hourly.get("time") or [])
    empty = pd.DataFrame(
        columns=[
            "city_name",
            "country",
            "time",
            "date",
            "temperature_2m",
            "precipitation",
        ]
    )
    if not times:
        logger.warning("%s: hourly forecast has no timestamps", city)
        return empty

    frame = pd.DataFrame(
        {
            "time": times,
            "temperature_2m": _pad_series(hourly.get("temperature_2m"), len(times)),
            "precipitation": _pad_series(hourly.get("precipitation"), len(times)),
        }
    )
    frame["time"] = pd.to_datetime(frame["time"], errors="coerce")
    invalid_times = int(frame["time"].isna().sum())
    if invalid_times:
        logger.warning(
            "%s: dropping %d hourly rows with invalid timestamps", city, invalid_times
        )
        frame = frame.dropna(subset=["time"])
    if frame.empty:
        return empty

    frame["temperature_2m"] = pd.to_numeric(frame["temperature_2m"], errors="coerce")
    frame["precipitation"] = pd.to_numeric(frame["precipitation"], errors="coerce")
    missing_temp = int(frame["temperature_2m"].isna().sum())
    missing_precip = int(frame["precipitation"].isna().sum())
    if missing_temp or missing_precip:
        logger.warning(
            "%s: %d missing temperatures, %d missing precipitation values",
            city,
            missing_temp,
            missing_precip,
        )

    frame["date"] = frame["time"].dt.normalize()
    frame["city_name"] = city
    frame["country"] = payload.get("country")
    return frame[
        ["city_name", "country", "time", "date", "temperature_2m", "precipitation"]
    ]


def transform(payloads: list[dict], cities: pd.DataFrame) -> pd.DataFrame:
    """Load hourly forecasts, aggregate per city-day, and join CSV city names."""
    frames = [_hourly_frame(payload) for payload in payloads]
    hourly = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if hourly.empty:
        logger.error("No hourly forecast rows to transform")
        raise ValueError("No hourly forecast rows to transform")

    logger.info("Loaded %d hourly rows from %d JSON payloads", len(hourly), len(payloads))

    daily = hourly.groupby(["city_name", "country", "date"], as_index=False).agg(
        max_temperature=("temperature_2m", "max"),
        precipitation_sum=("precipitation", "sum"),
    )
    logger.info(
        "Aggregated max temperature and precipitation sum into %d city-day rows",
        len(daily),
    )

    mapped = cities.merge(daily, on=["city_name", "country"], how="left")
    unmatched = mapped.loc[mapped["date"].isna(), "city_name"]
    if not unmatched.empty:
        logger.warning(
            "No weather stats for %d cities: %s",
            unmatched.nunique(),
            ", ".join(unmatched.dropna().unique()),
        )

    mapped = mapped.sort_values(["city_name", "date"], na_position="last").reset_index(
        drop=True
    )
    logger.info(
        "Joined normalized CSV city names onto %d aggregated weather rows",
        int(mapped["date"].notna().sum()),
    )
    return mapped


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
    try:
        report = transform(forecasts, cities)
    except Exception:
        logger.exception("Pipeline aborted while transforming forecasts")
        raise
    logger.info(
        "Report columns %s; date dtype %s",
        list(report.columns),
        report["date"].dtype,
    )
    logger.info("Pipeline run finished with %d city-day rows", len(report))


if __name__ == "__main__":
    asyncio.run(main())
