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
  - name: trip_id
    type: bigint
    description: Surrogate row key (ROW_NUMBER over the window)
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: Trip pickup datetime
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Trip dropoff datetime
  - name: passenger_count
    type: integer
    description: Number of passengers reported
  - name: trip_distance
    type: float
    description: Trip distance in miles
    checks:
      - name: non_negative
  - name: ratecode_id
    type: integer
    description: Rate code ID
  - name: store_and_fwd_flag
    type: string
    description: Store and forward flag
  - name: pu_location_id
    type: integer
    description: Pickup TLC zone location ID
  - name: do_location_id
    type: integer
    description: Dropoff TLC zone location ID
  - name: payment_type_id
    type: integer
    description: Payment type numeric code
  - name: payment_type_name
    type: string
    description: Human-readable payment type from lookup table
  - name: fare_amount
    type: float
    description: Metered fare amount
  - name: extra
    type: float
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float
    description: MTA tax
  - name: tip_amount
    type: float
    description: Tip amount
  - name: tolls_amount
    type: float
    description: Total tolls paid
  - name: improvement_surcharge
    type: float
    description: Improvement surcharge
  - name: total_amount
    type: float
    description: Total amount charged
    checks:
      - name: non_negative
  - name: congestion_surcharge
    type: float
    description: Congestion surcharge
  - name: taxi_type
    type: string
    description: Taxi type (yellow or green)
  - name: extracted_at
    type: timestamp
    description: Timestamp when the record was extracted from source

custom_checks:
  - name: no_negative_fares
    description: Valid trips should not have negative fare amounts
    query: |
      SELECT COUNT(*)
      FROM staging.trips
      WHERE fare_amount < 0
    value: 0

@bruin */

WITH raw AS (
    SELECT
        COALESCE(tpep_pickup_datetime, lpep_pickup_datetime)   AS pickup_datetime,
        COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) AS dropoff_datetime,
        passenger_count,
        trip_distance,
        "RatecodeID"                                           AS ratecode_id,
        store_and_fwd_flag,
        "PULocationID"                                         AS pu_location_id,
        "DOLocationID"                                         AS do_location_id,
        payment_type                                           AS payment_type_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        taxi_type,
        extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY
                COALESCE(tpep_pickup_datetime, lpep_pickup_datetime),
                COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime),
                "PULocationID",
                "DOLocationID",
                fare_amount,
                taxi_type
            ORDER BY extracted_at DESC
        ) AS rn
    FROM ingestion.trips
    WHERE COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) >= '{{ start_datetime }}'
      AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) < '{{ end_datetime }}'
      AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) IS NOT NULL
      AND COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) IS NOT NULL
),
deduped AS (
    SELECT *
    FROM raw
    WHERE rn = 1
)
SELECT
    ROW_NUMBER() OVER (ORDER BY pickup_datetime, pu_location_id, do_location_id) AS trip_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    d.payment_type_id,
    COALESCE(pl.payment_type_name, 'unknown')                                     AS payment_type_name,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    d.taxi_type,
    d.extracted_at
FROM deduped d
LEFT JOIN ingestion.payment_lookup pl
    ON d.payment_type_id = pl.payment_type_id
