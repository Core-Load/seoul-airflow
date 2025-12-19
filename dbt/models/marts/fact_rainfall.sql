SELECT
    r.rf_cd,          -- 강우량계 코드
    r.gu_cd,          -- 구청 코드
    r.rn_10m,         -- 10분우량
    r.data_clct_tm,   -- 자료수집 시각
    CASE
        WHEN r.rn_10m = 0 OR r.rn_10m IS NULL THEN 'none'       -- 초록색: 비 안옴
        WHEN r.rn_10m > 0 AND r.rn_10m <= 1 THEN 'light'        -- 노란색: 약한 비
        WHEN r.rn_10m > 1 AND r.rn_10m <= 5 THEN 'moderate'     -- 주황색: 보통 비
        WHEN r.rn_10m > 5 THEN 'heavy'                          -- 빨간색: 강한 비
    END AS rainfall_level,
    CASE
        WHEN r.rn_10m = 0 OR r.rn_10m IS NULL THEN '강우없음'
        WHEN r.rn_10m > 0 AND r.rn_10m <= 1 THEN '약한비'
        WHEN r.rn_10m > 1 AND r.rn_10m <= 5 THEN '보통비'
        WHEN r.rn_10m > 5 THEN '강한비'
    END AS rainfall_level_kr,
FROM {{ ref('int_rainfall') }} as r