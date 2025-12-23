WITH recent_dates AS (
    SELECT DISTINCT msrmt_date
    FROM {{ ref('stg_daily_air_quality') }}
    ORDER BY msrmt_date DESC
    LIMIT 2
),
base AS (
    SELECT
        s.msrmt_date,
        s.msrstn_nm,
        s.no2_ppm,
        s.ozone_ppm,
        s.co_ppm,
        s.so2_ppm,
        s.pm10,
        s.pm25
    FROM {{ ref('stg_daily_air_quality') }} s
    INNER JOIN recent_dates r ON s.msrmt_date = r.msrmt_date
),
with_prev AS (
    SELECT
        *,
        lag(pm10) OVER (
            PARTITION BY msrstn_nm
            ORDER BY msrmt_date
        ) AS prev_pm10,
        lag(pm25) OVER (
            PARTITION BY msrstn_nm
            ORDER BY msrmt_date
        ) AS prev_pm25,
        lag(msrmt_date) OVER (
            PARTITION BY msrstn_nm
            ORDER BY msrmt_date
        ) AS prev_measure_date
    FROM base
)
SELECT
    msrmt_date,
    msrstn_nm,
    no2_ppm,
    ozone_ppm,
    co_ppm,
    so2_ppm,
    pm10,
    pm25,
    prev_measure_date,
    prev_pm10,
    prev_pm25,
    pm10 - prev_pm10  AS pm10_diff,
    pm25 - prev_pm25  AS pm25_diff
FROM with_prev