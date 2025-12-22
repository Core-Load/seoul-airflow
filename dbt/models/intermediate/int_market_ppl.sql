WITH staging_market_ppl AS (
    -- 이전에 만든 stg_market_3q_info 모델을 불러옵니다.
    -- 파일명(모델명)을 따옴표 안에 적어주면 됩니다.
    SELECT * FROM {{ ref('stg_market_ppl') }}
)

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

FROM staging_market_ppl