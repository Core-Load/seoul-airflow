{{
    config(
        materialized='table',
        unique_key=['msrmt_date', 'msrstn_nm']
    )
}}
WITH latest_date AS (
    SELECT max(msrmt_date) AS max_msrmt_date
    FROM {{ ref('int_air_quality') }}
)
SELECT
    a.msrmt_date,   -- 측정일
    a.msrstn_nm,    -- 측정소명
    a.no2_ppm,      -- 이산화질소
    a.ozone_ppm,    -- 오존
    a.co_ppm,       -- 일산화탄소
    a.so2_ppm,      -- 아황산가스
    a.pm10,         -- 미세먼지
    a.pm25,         -- 초미세먼지
    a.prev_measure_date,    -- 비교 기준일
    a.pm10_diff,    -- 기준일 대비 미세먼지 증감
    a.pm25_diff,    -- 기준일 대비 초미세먼지 증감
    CASE
        WHEN a.pm10_diff > 0 THEN 'increase'
        WHEN a.pm10_diff < 0 THEN 'decrease'
        ELSE 'same'
    END AS pm10_trend,
    CASE
        WHEN a.pm25_diff > 0 THEN 'increase'
        WHEN a.pm25_diff < 0 THEN 'decrease'
        ELSE 'same'
    END AS pm25_trend,
    CASE
        WHEN pm10 <= 30 THEN 'GOOD'
        WHEN pm10 <= 80 THEN 'NORMAL'
        WHEN pm10 <= 150 THEN 'BAD'
        ELSE 'VERY_BAD'
    END AS pm10_grade,
    CASE
        WHEN pm25 <= 15 THEN 'GOOD'
        WHEN pm25 <= 35 THEN 'NORMAL'
        WHEN pm25 <= 75 THEN 'BAD'
        ELSE 'VERY_BAD'
    END AS pm25_grade
FROM {{ ref('int_air_quality') }} a
JOIN latest_date l ON a.msrmt_date = l.max_msrmt_date