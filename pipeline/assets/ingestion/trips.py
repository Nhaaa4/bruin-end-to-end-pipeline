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
    start_date = os.environ.get("BRUIN_START_DATE", "2019-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2019-02-01")
    bruin_vars = os.environ.get("BRUIN_VARS", "{}")
    variables = json.loads(bruin_vars)
    taxi_types = variables.get("taxi_types", ["yellow", "green"])

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    months = []
    current = date(start.year, start.month, 1)
    while current < end:
        months.append(current)
        current += relativedelta(months=1)

    frames = []
    errors = []
    for taxi_type in taxi_types:
        for month in months:
            filename = f"{taxi_type}_tripdata_{month.year}-{month.month:02d}.csv.gz"
            url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{filename}"
            print(f"Fetching {url}")
            try:
                response = requests.get(url, timeout=180, allow_redirects=True)
                response.raise_for_status()
                # Read CSV (gzip-compressed), convert to parquet in-memory for proper type inference
                df = pd.read_csv(BytesIO(response.content), compression="gzip", low_memory=False)
                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)
                df = pd.read_parquet(parquet_buffer)
                df["taxi_type"] = taxi_type
                df["extracted_at"] = datetime.utcnow()
                print(f"Fetched {len(df)} rows from {url}")
                frames.append(df)
            except Exception as e:
                errors.append(f"{url}: {e}")
                print(f"ERROR fetching {url}: {e}")

    if errors and not frames:
        raise RuntimeError(
            f"All fetch attempts failed ({len(errors)} errors):\n" + "\n".join(errors)
        )

    if not frames:
        raise RuntimeError("No data fetched — check taxi_types and date range.")

    return pd.concat(frames, ignore_index=True)



