WITH intermediate_data AS (
    SELECT * FROM {{ref('int_market_store')}}
)

SELECT
    area_name,
    major_category,
    medium_category,
    busyness,
    payment_count,
    payment_min,
    payment_max,
    merchant_count,
    loaded_at

FROM intermediate_data