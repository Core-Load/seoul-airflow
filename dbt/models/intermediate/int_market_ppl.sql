{{config(materialized='ephemeral')}}

SELECT
    area_name,
    commerce_lvl,
    female_rate,
    male_rate,
    age_10_rate,
    age_20_rate,
    age_30_rate,
    age_40_rate,
    age_50_rate,
    age_60_rate,
    personal_rate,
    corporation_rate,
    commerce_time,
    loaded_at

FROM {{ref('stg_market_ppl')}}