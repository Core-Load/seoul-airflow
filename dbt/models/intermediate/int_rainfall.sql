{{
    config(
        materialized='incremental',
        unique_key='rf_cd',
        on_schema_change='append_new_columns'
    )
}}
SELECT DISTINCT ON (rf_cd)
    rf_cd,
    rf_nm,
    gu_cd,
    gu_nm,
    rn_10m,
    data_clct_tm,
    updated_at
FROM {{ ref('stg_list_rainfall_service') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}
ORDER BY rf_cd, updated_at DESC