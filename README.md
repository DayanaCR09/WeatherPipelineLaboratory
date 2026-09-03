# WeatherPipelineLaboratory

An asynchronous pipeline that fetches weather data, cleans it into tabular form,
and exports an Excel report.

## Layout

| Path | Purpose |
| --- | --- |
| `src/pipeline.py` | Main pipeline: fetch, transform, export |
| `tests/` | Test suite |
| `data/` | Input datasets |
| `reports/` | Generated Excel output (not tracked) |

## Setup

```bash
uv sync
cp .env.example .env
uv run python src/pipeline.py
```

## Configuration

Runtime settings are read from a local `.env` file, which is not committed.
See `.env.example` for the required variables: `WEATHER_UNIT`, `MAX_RETRIES`,
`LOG_LEVEL`, and `ALERT_THRESHOLD_C`.

## Development

```bash
uv run pytest
uv run ruff check .
```
