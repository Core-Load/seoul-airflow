WITH intermediate_data AS (
    SELECT * FROM {{ref('int_market_ppl')}}
)

SELECT * FROM intermediate_data