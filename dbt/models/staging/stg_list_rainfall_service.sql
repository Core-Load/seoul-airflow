SELECT
    rf_cd,
    rf_nm,
    gu_cd,
    gu_nm,
    rn_10m,
    data_clct_tm,
    created_at,
    updated_at
FROM {{ source('raw_data', 'list_rainfall_service') }}