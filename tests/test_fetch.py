"""Intercept HTTP GET with MockTransport so Open-Meteo is never contacted."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pandas as pd
import pytest

from pipeline import FORECAST_URL, _forecast_params, _get_forecast, fetch

FORECAST_FIXTURE = Path(__file__).parent / "fixtures" / "open_meteo_forecast.json"

Handler = Callable[[httpx.Request], httpx.Response]


def _hourly_payload() -> dict:
    return json.loads(FORECAST_FIXTURE.read_text(encoding="utf-8"))


def _cities(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def _tokyo() -> dict:
    return {
        "city_name": "Tokyo",
        "country": "Japan",
        "latitude": 35.6895,
        "longitude": 139.6917,
    }


def _paris() -> dict:
    return {
        "city_name": "Paris",
        "country": "France",
        "latitude": 48.8566,
        "longitude": 2.3522,
    }


def _json_response(payload: dict, status_code: int = 200) -> httpx.Response:
    return httpx.Response(status_code, json=payload)


def _text_response(text: str, status_code: int) -> httpx.Response:
    return httpx.Response(status_code, text=text)


class OpenMeteoMock:
    """Script HTTP GET responses and record requests before they leave the client."""

    def __init__(self, *outcomes: httpx.Response | BaseException) -> None:
        self._outcomes = list(outcomes)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert request.method == "GET"
        assert request.url.host == "api.open-meteo.com"
        assert request.url.path == "/v1/forecast"
        if not self._outcomes:
            raise AssertionError("Unexpected extra GET toward Open-Meteo")
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, BaseException):
            if isinstance(outcome, httpx.RequestError):
                raise outcome.__class__(str(outcome), request=request)
            raise outcome
        return outcome

    def params(self, index: int = 0) -> httpx.QueryParams:
        return self.requests[index].url.params


def _client(handler: Handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_get_sends_hourly_query_before_any_network_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRIES", "1")
    monkeypatch.setenv("WEATHER_UNIT", "celsius")
    mock = OpenMeteoMock(_json_response(_hourly_payload()))

    async with _client(mock) as client:
        payload = await _get_forecast(
            client, "Tokyo", _forecast_params(35.6895, 139.6917)
        )

    assert len(mock.requests) == 1
    params = mock.params()
    assert params["hourly"] == "temperature_2m,precipitation"
    assert params["timezone"] == "auto"
    assert params["temperature_unit"] == "celsius"
    assert float(params["latitude"]) == pytest.approx(35.6895)
    assert float(params["longitude"]) == pytest.approx(139.6917)
    assert payload["hourly"]["temperature_2m"] == [21.0, 22.5]


@pytest.mark.asyncio
async def test_fetch_requests_hourly_forecasts_sequentially(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRIES", "1")
    monkeypatch.setenv("WEATHER_UNIT", "celsius")
    body = _hourly_payload()
    mock = OpenMeteoMock(_json_response(body), _json_response(body))

    async with _client(mock) as client:
        results = await fetch(client, _cities(_tokyo(), _paris()))

    assert len(mock.requests) == 2
    assert float(mock.params(0)["latitude"]) == pytest.approx(35.6895)
    assert float(mock.params(1)["latitude"]) == pytest.approx(48.8566)
    assert [row["city_name"] for row in results] == ["Tokyo", "Paris"]
    assert results[0]["country"] == "Japan"
    assert results[0]["hourly"]["precipitation"] == [0.0, 0.2]


@pytest.mark.asyncio
async def test_fetch_uses_configured_temperature_unit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRIES", "1")
    monkeypatch.setenv("WEATHER_UNIT", "fahrenheit")
    mock = OpenMeteoMock(_json_response(_hourly_payload()))

    async with _client(mock) as client:
        await fetch(client, _cities(_tokyo()))

    assert mock.params()["temperature_unit"] == "fahrenheit"


@pytest.mark.asyncio
async def test_get_forecast_rejects_400_json_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRIES", "3")
    mock = OpenMeteoMock(
        _json_response({"reason": "Latitude must be in range of -90 to 90"}, 400)
    )

    async with _client(mock) as client:
        with pytest.raises(
            RuntimeError,
            match="Open-Meteo rejected Tokyo: Latitude must be in range of -90 to 90",
        ):
            await _get_forecast(client, "Tokyo", _forecast_params(35.6895, 139.6917))

    assert len(mock.requests) == 1


@pytest.mark.asyncio
async def test_get_forecast_rejects_400_plain_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRIES", "1")
    mock = OpenMeteoMock(_text_response("malformed request", 400))

    async with _client(mock) as client:
        with pytest.raises(
            RuntimeError, match="Open-Meteo rejected Tokyo: malformed request"
        ):
            await _get_forecast(client, "Tokyo", _forecast_params(35.6895, 139.6917))


@pytest.mark.asyncio
async def test_get_forecast_rejects_error_flag_in_200_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRIES", "1")
    mock = OpenMeteoMock(
        _json_response({"error": True, "reason": "Hourly API request limit exceeded"})
    )

    async with _client(mock) as client:
        with pytest.raises(RuntimeError, match="Hourly API request limit exceeded"):
            await _get_forecast(client, "Tokyo", _forecast_params(35.6895, 139.6917))


@pytest.mark.asyncio
async def test_fetch_retries_transient_open_meteo_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRIES", "3")
    monkeypatch.setenv("WEATHER_UNIT", "celsius")
    monkeypatch.setattr("pipeline.asyncio.sleep", AsyncMock())
    mock = OpenMeteoMock(
        _json_response({}, status_code=503),
        _json_response(_hourly_payload()),
    )

    async with _client(mock) as client:
        results = await fetch(client, _cities(_tokyo()))

    assert len(mock.requests) == 2
    assert results[0]["city_name"] == "Tokyo"


@pytest.mark.asyncio
async def test_fetch_retries_connect_error_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRIES", "3")
    monkeypatch.setenv("WEATHER_UNIT", "celsius")
    monkeypatch.setattr("pipeline.asyncio.sleep", AsyncMock())
    mock = OpenMeteoMock(
        httpx.ConnectError(
            "connection refused", request=httpx.Request("GET", FORECAST_URL)
        ),
        _json_response(_hourly_payload()),
    )

    async with _client(mock) as client:
        results = await fetch(client, _cities(_tokyo()))

    assert len(mock.requests) == 2
    assert results[0]["hourly"]["time"] == ["2026-09-08T00:00", "2026-09-08T01:00"]


@pytest.mark.asyncio
async def test_fetch_keeps_successful_city_when_another_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRIES", "1")
    monkeypatch.setenv("WEATHER_UNIT", "celsius")
    mock = OpenMeteoMock(
        _json_response({"reason": "Too many coordinates"}, 400),
        _json_response(_hourly_payload()),
    )

    async with _client(mock) as client:
        results = await fetch(client, _cities(_tokyo(), _paris()))

    assert [row["city_name"] for row in results] == ["Paris"]
    assert len(mock.requests) == 2


@pytest.mark.asyncio
async def test_fetch_raises_when_retries_are_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RETRIES", "2")
    monkeypatch.setenv("WEATHER_UNIT", "celsius")
    monkeypatch.setattr("pipeline.asyncio.sleep", AsyncMock())
    mock = OpenMeteoMock(
        _json_response({}, status_code=429),
        _json_response({}, status_code=429),
    )

    async with _client(mock) as client:
        with pytest.raises(
            RuntimeError, match="No forecasts were retrieved from Open-Meteo"
        ):
            await fetch(client, _cities(_tokyo()))

    assert len(mock.requests) == 2


@pytest.mark.asyncio
async def test_fetch_rejects_empty_city_list() -> None:
    mock = OpenMeteoMock()
    async with _client(mock) as client:
        with pytest.raises(
            ValueError, match="No cities available to fetch forecasts for"
        ):
            await fetch(client, _cities())
    assert mock.requests == []
