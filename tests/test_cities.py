"""Unit tests for CSV city-name normalization."""

from pathlib import Path

import pytest

from cities import load_cities, normalize_city, normalize_column, normalize_country


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("  new york  ", "New York"),
        ("LONDON", "London"),
        ("tOkYo!!", "Tokyo"),
        ("  São   Paulo", "São Paulo"),
        ("mumbai (bombay)", "Mumbai"),
        ("sydney***", "Sydney"),
        ("mexico city, cdmx", "Mexico City"),
        ("seoul#1", "Seoul"),
        ("istanbul (İstanbul) ", "Istanbul"),
        ("buenos    aires", "Buenos Aires"),
        (" zürich", "Zürich"),
        ("cape town!", "Cape Town"),
        ("reykjavík ", "Reykjavík"),
        ("bei jing / 北京", "Beijing"),
        ("", ""),
    ],
)
def test_normalize_city(raw: str, expected: str) -> None:
    assert normalize_city(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("u.s.a.", "United States"),
        ("United Kingdom  ", "United Kingdom"),
        ("BRAZIL", "Brazil"),
        ("méxico", "México"),
        ("South  KOREA", "South Korea"),
        ("türkiye", "Türkiye"),
    ],
)
def test_normalize_country(raw: str, expected: str) -> None:
    assert normalize_country(raw) == expected


def test_normalize_column_strips_header_noise() -> None:
    assert normalize_column(" LONGITUDE") == "longitude"
    assert normalize_column("City_Name ") == "city_name"


def test_load_cities_normalizes_and_drops_invalid_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "raw_cities.csv"
    csv_path.write_text(
        "City_Name , country ,Latitude, LONGITUDE\n"
        "tOkYo!!,japan ,35.6895,139.6917\n"
        "broken,Spain,not-a-number,-3.7\n"
        "  São   Paulo,BRAZIL,-23.5505,-46.6333\n"
        "nowhere,Spain,91.0,0.0\n",
        encoding="utf-8",
    )

    frame = load_cities(csv_path)

    assert list(frame["city_name"]) == ["Tokyo", "São Paulo"]
    assert list(frame["country"]) == ["Japan", "Brazil"]
    assert frame.loc[0, "latitude"] == pytest.approx(35.6895)


def test_load_cities_rejects_missing_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text("city_name,country\nTokyo,Japan\n", encoding="utf-8")
    with pytest.raises(KeyError, match="Missing required columns"):
        load_cities(csv_path)
