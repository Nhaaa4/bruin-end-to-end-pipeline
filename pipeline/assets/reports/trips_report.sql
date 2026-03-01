/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: trip_date
    type: date
    description: "Date of the trip (truncated from pickup_datetime)"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow or green)"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Human-readable payment type label"
    primary_key: true
  - name: total_trips
    type: integer
    description: "Number of trips in this group"
    checks:
      - name: not_null
      - name: positive
  - name: total_revenue
    type: float
    description: "Sum of total_amount for this group"
    checks:
      - name: non_negative
  - name: avg_fare
    type: float
    description: "Average fare amount for this group"
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: float
    description: "Average trip distance for this group"
    checks:
      - name: non_negative

@bruin */

SELECT
  CAST(pickup_datetime AS DATE)    AS trip_date,
  taxi_type,
  COALESCE(payment_type_name, 'unknown') AS payment_type_name,
  COUNT(*)                         AS total_trips,
  SUM(total_amount)                AS total_revenue,
  AVG(fare_amount)                 AS avg_fare,
  AVG(trip_distance)               AS avg_trip_distance
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3

