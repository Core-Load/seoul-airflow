{{ config(materialized='view') }}

SELECT
    road_name,

    /* 해당 도로에서 가장 혼잡한 상태 */
    MAX(congestion_score) AS congestion_score,

    /* 가장 느린 구간 기준 속도 */
    MIN(speed_kmh) AS min_speed_kmh,

    /* 상태 라벨 */
    CASE
        WHEN MAX(congestion_score) = 3 THEN 'CONGESTED'
        WHEN MAX(congestion_score) = 2 THEN 'NORMAL'
        ELSE 'SMOOTH'
    END AS traffic_flow_status,

    /* 혼잡 구간 개수 */
    COUNT(*) FILTER (WHERE congestion_score = 3) AS congested_link_cnt,

    /* 전체 구간 수 */
    COUNT(*) AS total_link_cnt

FROM {{ ref('int_traffic_road') }}
GROUP BY road_name
