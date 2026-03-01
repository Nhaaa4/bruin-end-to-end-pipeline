/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the trip ended"
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "Pickup location ID"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: "Dropoff location ID"
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Base fare amount in USD"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow or green)"
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Human-readable payment type label"
  - name: total_amount
    type: float
    description: "Total trip amount in USD"
    checks:
      - name: non_negative

custom_checks:
  - name: no_duplicate_trips
    description: "Ensure no duplicate trips exist after deduplication"
    query: |
      SELECT COUNT(*) FROM (
        SELECT pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount, COUNT(*) as cnt
        FROM staging.trips
        GROUP BY 1,2,3,4,5
        HAVING cnt > 1
      )
    value: 0

@bruin */

WITH raw AS (
  SELECT *
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
),

deduplicated AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount
      ORDER BY extracted_at DESC
    ) AS rn
  FROM raw
  WHERE pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND pickup_location_id IS NOT NULL
    AND dropoff_location_id IS NOT NULL
    AND fare_amount IS NOT NULL
)

SELECT
  d.pickup_datetime,
  d.dropoff_datetime,
  d.pickup_location_id,
  d.dropoff_location_id,
  d.fare_amount,
  d.total_amount,
  d.taxi_type,
  d.passenger_count,
  d.trip_distance,
  d.payment_type,
  p.payment_type_name,
  d.extracted_at
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup p
  ON d.payment_type = p.payment_type_id
WHERE d.rn = 1

