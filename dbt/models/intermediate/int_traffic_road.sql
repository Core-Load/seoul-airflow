{{ config(materialized='view') }}

WITH base AS (
    SELECT
        link_id,
        road_name,
        start_nd_cd,
        start_nd_name,
        end_nd_cd,
        end_nd_name,
        distance_m,
        speed_kmh,
        congestion_status,
        xylist_raw
    FROM {{ ref('stg_traffic_road') }}
    WHERE speed_kmh IS NOT NULL
)

SELECT
    link_id,
    road_name,
    start_nd_cd,
    start_nd_name,
    end_nd_cd,
    end_nd_name,
    distance_m,
    speed_kmh,
    congestion_status,

    CASE
        WHEN speed_kmh >= 40 THEN 'SMOOTH'
        WHEN speed_kmh >= 20 THEN 'NORMAL'
        ELSE 'CONGESTED'
    END AS traffic_flow_status,

    CASE
        WHEN speed_kmh >= 40 THEN 1
        WHEN speed_kmh >= 20 THEN 2
        ELSE 3
    END AS congestion_score,

    xylist_raw
FROM base
