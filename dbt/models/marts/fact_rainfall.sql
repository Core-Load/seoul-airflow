SELECT
    rf_cd,          -- 강우량계 코드
    gu_cd,          -- 구청 코드
    rn_10m,         -- 10분우량
    data_clct_tm    -- 자료수집 시각
FROM {{ ref('int_rainfall') }}