"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: string
    description: The taxi type (yellow or green)
  - name: extracted_at
    type: timestamp
    description: Timestamp when the record was extracted

@bruin"""

import io
import json
import os
from datetime import datetime, timezone

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


def materialize():
    """
    Fetch NYC taxi parquet data from the TLC endpoint for each taxi_type
    and each calendar month within the BRUIN_START_DATE / BRUIN_END_DATE window.
    Returns a concatenated DataFrame in raw (untransformed) format.
    """
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    start_date = datetime.strptime(os.environ["BRUIN_START_DATE"], "%Y-%m-%d")
    end_date = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d")

    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    extracted_at = datetime.now(timezone.utc)

    frames = []
    current = start_date.replace(day=1)

    while current < end_date:
        year = current.strftime("%Y")
        month = current.strftime("%m")

        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year}-{month}.parquet"
            url = base_url + filename
            print(f"Fetching: {url}")

            try:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                }
                response = requests.get(url, headers=headers, timeout=300)
                response.raise_for_status()
                df = pd.read_parquet(io.BytesIO(response.content))
                df["taxi_type"] = taxi_type
                df["extracted_at"] = extracted_at
                frames.append(df)
                print(f"  -> {len(df):,} rows")
            except Exception as exc:
                print(f"  -> WARN: could not fetch {url}: {exc}")

        current += relativedelta(months=1)

    if not frames:
        print("No data fetched for the given window — skipping load.")
        return None

    return pd.concat(frames, ignore_index=True)


