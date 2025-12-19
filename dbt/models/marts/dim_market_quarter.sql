WITH intermediate_data AS (
    SELECT * FROM {{ref('int_market_quarter')}}
)

SELECT
    stdr_yyqu_cd,
    trdar_se_cd_nm,
    trdar_cd,       -- fact와 연결
    trdar_cd_nm,
    svc_induty_cd_nm,
    thsmon_selng_co,       -- fact와 연결
    thsmon_selng_amt       -- fact와 연결
FROM intermediate_data
