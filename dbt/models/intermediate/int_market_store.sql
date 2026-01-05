{{config(materialized='ephemeral')}}

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
FROM {{ref('staging_market_store')}}
