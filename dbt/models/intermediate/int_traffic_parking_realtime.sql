{{ config(materialized='view') }}

WITH base AS (
    SELECT
        parking_name,
        parking_code,
        total_capacity,
        lat,
        lng,
        pay_yn,
        real_time_available,
        observed_at
    FROM {{ ref('stg_traffic_parking_realtime') }}
    WHERE total_capacity IS NOT NULL
      AND total_capacity > 0
      AND lat IS NOT NULL
      AND lng IS NOT NULL
)

SELECT
    parking_name,
    parking_code,
    total_capacity,
    lat,
    lng,
    pay_yn,
    real_time_available,
    observed_at,

    CASE
        WHEN total_capacity >= 300 THEN 'XL'
        WHEN total_capacity >= 150 THEN 'L'
        WHEN total_capacity >= 50  THEN 'M'
        ELSE 'S'
    END AS capacity_size,

    CASE
        WHEN total_capacity >= 300 THEN 30
        WHEN total_capacity >= 150 THEN 24
        WHEN total_capacity >= 50  THEN 16
        ELSE 10
    END AS marker_size

FROM base
