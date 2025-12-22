WITH staging_market_store AS (
    -- 이전에 만든 stg_market_3q_info 모델을 불러옵니다.
    -- 파일명(모델명)을 따옴표 안에 적어주면 됩니다.
    SELECT * FROM {{ ref('stg_market_store') }}
)

SELECT
    area_name,
    major_category,
    medium_category,
    busyness,
    payment_count,
    payment_min,
    payment_max,
    merchant_count
    loaded_at

FROM staging_market_store