"""Unit tests for Excel and JSON report export."""

import json
from pathlib import Path

import pandas as pd

from pipeline import ALERT_REPORT_NAME, EXCEL_REPORT_NAME, write_reports


def test_write_reports_creates_excel_and_alert_json(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        [
            {
                "city_name": "Cairo",
                "country": "Egypt",
                "latitude": 30.0444,
                "longitude": 31.2357,
                "date": pd.Timestamp("2026-09-08"),
                "max_temperature": 35.9,
                "precipitation_sum": 0.0,
            },
            {
                "city_name": "London",
                "country": "United Kingdom",
                "latitude": 51.5074,
                "longitude": -0.1278,
                "date": pd.Timestamp("2026-09-08"),
                "max_temperature": 18.0,
                "precipitation_sum": 4.1,
            },
        ]
    )

    excel_path, json_path = write_reports(frame, tmp_path)

    assert excel_path == tmp_path / EXCEL_REPORT_NAME
    assert json_path == tmp_path / ALERT_REPORT_NAME
    assert excel_path.exists()
    assert excel_path.stat().st_size > 0

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["threshold_c"] == 30.0
    assert payload["city_count"] == 1
    assert payload["cities"][0]["city_name"] == "Cairo"
