SELECT
    to_date(msrmt_dt, 'YYYYMMDD') AS msrmt_date,    -- 측정일(date)
    msrmt_dt,            -- 측정일
    msrstn_nm,           -- 측정소명
    ntdx AS no2_ppm,     -- 이산화질소
    ozon AS ozone_ppm,   -- 오존
    cbmx AS co_ppm,      -- 일산화탄소
    spdx AS so2_ppm,     -- 아황산가스
    pm   AS pm10,        -- 미세먼지
    fpm  AS pm25         -- 초미세먼지
FROM {{ source('raw_data', 'daily_average_air_quality') }}
WHERE msrmt_dt IS NOT NULL
    AND msrstn_nm IS NOT NULL