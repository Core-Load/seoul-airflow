{{ config(materialized='view') }}

SELECT
    parking_name,
    parking_code,
    MAX(total_capacity) AS total_capacity,
    MAX(lat) AS lat,
    MAX(lng) AS lng,
    MAX(pay_yn) AS pay_yn,
    MAX(real_time_available) AS real_time_available,
    MAX(observed_at) AS observed_at,
    MAX(capacity_size) AS capacity_size,
    MAX(marker_size) AS marker_size
FROM {{ ref('int_traffic_parking_realtime') }}
GROUP BY parking_name, parking_code
ORDER BY total_capacity DESC
