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
  - name: pickup_date
    type: date
    description: Date of trip pickup
    primary_key: true
  - name: taxi_type
    type: string
    description: Taxi type (yellow or green)
    primary_key: true
  - name: payment_type_name
    type: string
    description: Human-readable payment type
    primary_key: true
  - name: pickup_datetime
    type: timestamp
    description: Truncated pickup datetime (window anchor for time_interval)
    primary_key: true
  - name: total_trips
    type: bigint
    description: Total number of trips in the group
    checks:
      - name: not_null
      - name: non_negative
  - name: total_passengers
    type: bigint
    description: Total passenger count
    checks:
      - name: non_negative
  - name: total_distance
    type: float
    description: Total trip distance in miles
    checks:
      - name: non_negative
  - name: total_fare
    type: float
    description: Total metered fare amount
  - name: total_amount
    type: float
    description: Total amount charged including all surcharges

@bruin */

SELECT
    CAST(pickup_datetime AS DATE) AS pickup_date,
    taxi_type,
    payment_type_name,
    DATE_TRUNC('day', pickup_datetime) AS pickup_datetime,
    COUNT(*)                       AS total_trips,
    SUM(passenger_count)           AS total_passengers,
    SUM(trip_distance)             AS total_distance,
    SUM(fare_amount)               AS total_fare,
    SUM(total_amount)              AS total_amount
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3, 4
