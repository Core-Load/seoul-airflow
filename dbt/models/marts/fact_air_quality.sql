SELECT
    msrmt_date,         -- 측정일
    msrstn_nm,          -- 측정소명
    no2_ppm,            -- 이산화질소
    ozone_ppm,          -- 오존
    co_ppm,             -- 일산화탄소
    so2_ppm,            -- 아황산가스
    pm10,               -- 미세먼지
    pm25,               -- 초미세먼지
    prev_measure_date,  -- 비교 기준일
    pm10_diff,          -- 기준일 대비 미세먼지 증감
    pm25_diff,          -- 기준일 대비 초미세먼지 증감
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
FROM {{ ref('int_air_quality') }}