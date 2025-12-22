WITH base AS (
    SELECT 
        area_name,
        created_at,
        -- 배열 데이터를 미리 추출
        data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' -> 'CMRCL_RSB' AS commerce_array
    FROM {{ source('raw_data', 'realtime_city_data') }}
)

SELECT
    area_name,
    (store ->> 'RSB_LRG_CTGR') as major_category,
    (store ->> 'RSB_MID_CTGR') as medium_category,
    (store ->> 'RSB_PAYMENT_LVL') as busyness,
    (store ->> 'RSB_SH_PAYMENT_CNT')::int as payment_count,
    (store ->> 'RSB_SH_PAYMENT_AMT_MIN')::int as payment_min,
    (store ->> 'RSB_SH_PAYMENT_AMT_MAX')::int as payment_max,
    (store ->> 'RSB_MCT_CNT')::int as merchant_count,
    created_at AS loaded_at
FROM base,
LATERAL jsonb_array_elements(commerce_array) AS store
