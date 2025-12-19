SELECT DISTINCT
    rf_cd,  -- 강우량계 코드
    rf_nm,  -- 강우량계명
    gu_cd,  -- 구청 코드
    gu_nm   -- 구청명
FROM {{ ref('int_rainfall') }}