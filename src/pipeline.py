"""Weather data pipeline: fetch, transform, and export."""

import asyncio
from pathlib import Path

import httpx
import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"


async def fetch(client: httpx.AsyncClient) -> dict:
    """Retrieve raw weather data from the source API."""
    raise NotImplementedError


def transform(payload: dict) -> pd.DataFrame:
    """Normalize the raw payload into a tabular form."""
    raise NotImplementedError


def export(df: pd.DataFrame, path: Path) -> None:
    """Write the transformed data to an Excel report."""
    df.to_excel(path, index=False)


async def main() -> None:
    load_dotenv()
    REPORTS_DIR.mkdir(exist_ok=True)

    async with httpx.AsyncClient() as client:
        payload = await fetch(client)

    export(transform(payload), REPORTS_DIR / "weather_report.xlsx")


if __name__ == "__main__":
    asyncio.run(main())
