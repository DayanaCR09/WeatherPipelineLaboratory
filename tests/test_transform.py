"""Unit tests for pandas hourly parsing, aggregation, and city joins."""

import pandas as pd
import pytest

from pipeline import _heat_alerts, transform


def _payload(
    city: str,
    country: str,
    times: list[str],
    temperatures: list[float | None],
    precipitation: list[float | None],
) -> dict:
    return {
        "city_name": city,
        "country": country,
        "hourly": {
            "time": times,
            "temperature_2m": temperatures,
            "precipitation": precipitation,
        },
    }


def test_transform_parses_datetimes_and_aggregates_daily_stats() -> None:
    payloads = [
        _payload(
            "Tokyo",
            "Japan",
            [
                "2026-09-08T00:00",
                "2026-09-08T01:00",
                "not-a-time",
                "2026-09-09T00:00",
            ],
            [20.0, None, 21.0, 22.0],
            [0.0, 1.5, 0.0, None],
        )
    ]
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

    report = transform(payloads, cities)

    assert pd.api.types.is_datetime64_any_dtype(report["date"])
    tokyo = report[report["city_name"] == "Tokyo"].sort_values("date")
    assert len(tokyo) == 2
    assert tokyo.iloc[0]["max_temperature"] == pytest.approx(20.0)
    assert tokyo.iloc[0]["precipitation_sum"] == pytest.approx(1.5)
    assert tokyo.iloc[1]["max_temperature"] == pytest.approx(22.0)
    assert tokyo.iloc[1]["precipitation_sum"] == pytest.approx(0.0)

    paris = report[report["city_name"] == "Paris"]
    assert len(paris) == 1
    assert paris.iloc[0]["country"] == "France"
    assert pd.isna(paris.iloc[0]["date"])


def test_heat_alerts_include_only_cities_above_30c() -> None:
    frame = pd.DataFrame(
        [
            {
                "city_name": "Cairo",
                "country": "Egypt",
                "date": pd.Timestamp("2026-09-08"),
                "max_temperature": 35.9,
            },
            {
                "city_name": "Cairo",
                "country": "Egypt",
                "date": pd.Timestamp("2026-09-09"),
                "max_temperature": 29.0,
            },
            {
                "city_name": "London",
                "country": "United Kingdom",
                "date": pd.Timestamp("2026-09-08"),
                "max_temperature": 30.0,
            },
        ]
    )

    payload = _heat_alerts(frame)

    assert payload["threshold_c"] == 30.0
    assert payload["city_count"] == 1
    assert payload["cities"][0]["city_name"] == "Cairo"
    assert payload["cities"][0]["max_temperature"] == 35.9
    assert payload["cities"][0]["days_over_threshold"] == 1
