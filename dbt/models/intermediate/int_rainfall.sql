{{
    config(
        materialized='incremental',
        unique_key='gu_cd',
        on_schema_change='append_new_columns'
    )
}}
SELECT DISTINCT ON (gu_cd)
    rf_cd,
    rf_nm,
    gu_cd,
    gu_nm,
    rn_10m,
    data_clct_tm,
    updated_at
FROM {{ ref('stg_list_rainfall_service') }}
{% if is_incremental() %}
WHERE data_clct_tm >= (SELECT MAX(data_clct_tm) - INTERVAL '1 hours' FROM {{ this }})
{% endif %}
ORDER BY 
    gu_cd,
    data_clct_tm DESC,  -- 1순위: 각 구별로 가장 최신 시간 데이터를 선택
    rn_10m DESC,        -- 2순위: 같은 시간이라면 강우량이 높은 것 선택
    rf_cd ASC           -- 3순위: 나머지 경우 코드 순