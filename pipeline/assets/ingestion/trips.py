"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default
materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started"
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the trip ended"
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow or green)"
    checks:
      - name: not_null
  - name: extracted_at
    type: timestamp
    description: "Timestamp when the record was extracted"

@bruin"""

import json
import os
from datetime import datetime, date
from io import BytesIO

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2022-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2022-02-01")
    bruin_vars = os.environ.get("BRUIN_VARS", "{}")
    variables = json.loads(bruin_vars)
    taxi_types = variables.get("taxi_types", ["yellow", "green"])

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    months = []
    current = date(start.year, start.month, 1)
    while current < end:
        months.append(current)
        current += relativedelta(months=1)

    frames = []
    for taxi_type in taxi_types:
        for month in months:
            filename = f"{taxi_type}_tripdata_{month.strftime('%Y-%m')}.parquet"
            url = base_url + filename
            print(f"Fetching {url}")
            try:
                response = requests.get(url, timeout=120)
                response.raise_for_status()
                df = pd.read_parquet(BytesIO(response.content))
                df["taxi_type"] = taxi_type
                df["extracted_at"] = datetime.utcnow()
                frames.append(df)
            except Exception as e:
                print(f"Warning: could not fetch {url}: {e}")

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)



