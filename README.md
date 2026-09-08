# Weather Pipeline Laboratory

An asynchronous Python pipeline that cleans a messy world-cities CSV, fetches hourly
forecasts from [Open-Meteo](https://open-meteo.com/), aggregates daily statistics with
pandas, and writes a formatted Excel report plus a heat-alert JSON payload.

## Requirements

- [uv](https://docs.astral.sh/uv/)
- Python 3.14 (installed automatically by `uv` from `.python-version`)

## Setup

```bash
uv sync
cp .env.example .env
```

`.env` is gitignored. The template includes:

| Variable | Purpose |
| --- | --- |
| `WEATHER_UNIT` | Open-Meteo `temperature_unit` (`celsius` or `fahrenheit`) |
| `MAX_RETRIES` | Attempts per city when Open-Meteo fails transiently |
| `LOG_LEVEL` | Logging verbosity written to `pipeline.log` |
| `ALERT_THRESHOLD_C` | Documented alert threshold (heat alerts use 30°C) |

## Run the pipeline

From the repository root:

```bash
uv run python src/pipeline.py
```

The run:

1. Normalizes `data/raw_cities.csv` (regex search/replace, strip, title-case).
2. Requests `hourly=temperature_2m,precipitation` with `timezone=auto` from
   `https://api.open-meteo.com/v1/forecast`, one city at a time.
3. Loads hourly JSON into pandas, converts timestamps, and aggregates **max
   temperature** and **precipitation sum** per city per day.
4. Joins those statistics back to the normalized CSV city names.

Outputs (gitignored):

| Path | Contents |
| --- | --- |
| `reports/weather_report.xlsx` | Formatted daily forecast workbook |
| `reports/heat_alerts.json` | Cities whose daily max is **> 30°C** |
| `pipeline.log` | INFO progress and ERROR detail |

## Tests and lint

```bash
uv run pytest
uv run ruff check .
uv run ruff format
```

Open-Meteo HTTP calls are mocked with `unittest.mock`, so pytest does not need
network access. Pull requests run the same Ruff and pytest commands in GitHub
Actions (`.github/workflows/ci.yml`).

## Project layout

| Path | Purpose |
| --- | --- |
| `src/pipeline.py` | Fetch, transform, and report |
| `src/cities.py` | CSV parsing and name normalization |
| `tests/` | pytest suite |
| `data/raw_cities.csv` | Input cities |
| `reports/` | Generated Excel and JSON (not tracked) |
