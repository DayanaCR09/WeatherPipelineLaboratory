"""Mocked Open-Meteo HTTP tests so the suite runs offline."""

from unittest.mock import AsyncMock, MagicMock

import httpx
import pandas as pd
import pytest

from pipeline import FORECAST_URL, fetch


def _json_response(payload: dict, status_code: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = payload
    response.text = ""
    if status_code >= 400:
        request = httpx.Request("GET", FORECAST_URL)
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error",
            request=request,
            response=httpx.Response(status_code, request=request),
        )
    else:
        response.raise_for_status.return_value = None
    return response


def _hourly_payload() -> dict:
    return {
        "hourly": {
            "time": ["2026-09-08T00:00", "2026-09-08T01:00"],
            "temperature_2m": [21.0, 22.5],
            "precipitation": [0.0, 0.2],
        }
    }


@pytest.mark.asyncio
async def test_fetch_requests_hourly_forecasts_sequentially(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRIES", "1")
    monkeypatch.setenv("WEATHER_UNIT", "celsius")

    cities = pd.DataFrame(
        [
            {
                "city_name": "Tokyo",
                "country": "Japan",
                "latitude": 35.6895,
                "longitude": 139.6917,
            },
            {
                "city_name": "Paris",
                "country": "France",
                "latitude": 48.8566,
                "longitude": 2.3522,
            },
        ]
    )
    client = MagicMock()
    client.get = AsyncMock(
        side_effect=[
            _json_response(_hourly_payload()),
            _json_response(_hourly_payload()),
        ]
    )

    results = await fetch(client, cities)

    assert client.get.await_count == 2
    first, second = client.get.await_args_list
    assert first.args[0] == FORECAST_URL
    assert second.args[0] == FORECAST_URL
    assert first.kwargs["params"]["hourly"] == "temperature_2m,precipitation"
    assert first.kwargs["params"]["timezone"] == "auto"
    assert first.kwargs["params"]["latitude"] == pytest.approx(35.6895)
    assert second.kwargs["params"]["latitude"] == pytest.approx(48.8566)
    assert [row["city_name"] for row in results] == ["Tokyo", "Paris"]
    assert results[0]["hourly"]["temperature_2m"] == [21.0, 22.5]


@pytest.mark.asyncio
async def test_fetch_retries_transient_open_meteo_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRIES", "3")
    monkeypatch.setenv("WEATHER_UNIT", "celsius")
    monkeypatch.setattr("pipeline.asyncio.sleep", AsyncMock())

    cities = pd.DataFrame(
        [
            {
                "city_name": "Cairo",
                "country": "Egypt",
                "latitude": 30.0444,
                "longitude": 31.2357,
            }
        ]
    )
    client = MagicMock()
    client.get = AsyncMock(
        side_effect=[
            _json_response({}, status_code=503),
            _json_response(_hourly_payload()),
        ]
    )

    results = await fetch(client, cities)

    assert client.get.await_count == 2
    assert results[0]["city_name"] == "Cairo"
