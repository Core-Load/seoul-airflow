WITH recent_dates AS (
    -- 원본 테이블에서 인덱스를 활용하여 최근 2일 찾기 (최신일 + 비교 기준일)
    SELECT DISTINCT msrmt_dt
    FROM {{ source('raw_data', 'daily_average_air_quality') }}
    ORDER BY msrmt_dt DESC
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
    WHERE s.msrmt_dt IN (SELECT msrmt_dt FROM recent_dates)
),
with_prev AS (
    SELECT
        b.*,
        lag(b.pm10) OVER (
            PARTITION BY b.msrstn_nm
            ORDER BY b.msrmt_date
        ) AS prev_pm10,
        lag(b.pm25) OVER (
            PARTITION BY b.msrstn_nm
            ORDER BY b.msrmt_date
        ) AS prev_pm25,
        lag(b.msrmt_date) OVER (
            PARTITION BY b.msrstn_nm
            ORDER BY b.msrmt_date
        ) AS prev_measure_date
    FROM base b
)
SELECT
    msrmt_date,     -- 측정일(date)
    msrstn_nm,      -- 측정소명
    no2_ppm,        -- 이산화질소농도(ppm)
    ozone_ppm,      -- 오존농도(ppm)
    co_ppm,         -- 일산화탄소농도(ppm)
    so2_ppm,        -- 아황산가스(ppm)
    pm10,           -- 미세먼지(㎍/㎥)
    pm25,           -- 초미세먼지(㎍/㎥)
    prev_measure_date,  -- 비교 기준일
    prev_pm10,          -- 기준일 미세먼지(㎍/㎥)
    prev_pm25,          -- 기준일 초미세먼지(㎍/㎥)
    pm10 - prev_pm10 AS pm10_diff, -- 기준일 대비 미세먼지 증감량
    pm25 - prev_pm25 AS pm25_diff  -- 기준일 대비 초미세먼지 증감량
FROM with_prev
WHERE msrmt_date = (SELECT max(msrmt_date) FROM base)