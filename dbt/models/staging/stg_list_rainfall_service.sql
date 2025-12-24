SELECT
    rf_cd,          -- 강우량계 코드
    rf_nm,          -- 강우량계명
    gu_cd,          -- 구청 코드
    gu_nm,          -- 구청명
    rn_10m,         -- 10분우량
    data_clct_tm,   -- 자료수집 시각
    created_at,
    updated_at
FROM {{ source('raw_data', 'list_rainfall_service') }}
WHERE data_clct_tm IS NOT NULL
    AND rf_cd IS NOT NULL