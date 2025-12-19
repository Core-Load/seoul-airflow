WITH staging_market_quarter_data AS (
    -- 이전에 만든 stg_market_3q_info 모델을 불러옵니다.
    -- 파일명(모델명)을 따옴표 안에 적어주면 됩니다.
    SELECT * FROM {{ ref('stg_market_3q_info') }}
)

SELECT 
    stdr_yyqu_cd,
    trdar_se_cd_nm,
    trdar_cd,
    trdar_cd_nm,
    svc_induty_cd_nm,
    thsmon_selng_co,
    thsmon_selng_amt,
    mdwk_selng_amt,
    wkend_selng_amt,
    -- 요일 별 매출액
    mon_selng_amt,
    tues_selng_amt,
    wed_selng_amt,
    thur_selng_amt,
    fri_selng_amt,
    sat_selng_amt,
    sun_selng_amt,
    -- 시간대 별 매출액
    tmzon_00_06_selng_amt,
    tmzon_06_11_selng_amt,
    tmzon_11_14_selng_amt,
    tmzon_14_17_selng_amt,
    tmzon_17_21_selng_amt,
    tmzon_21_24_selng_amt,
    -- 성별, 연령대별 매출액
    ml_selng_amt,
    fml_selng_amt,
    agrde_10_selng_amt,
    agrde_20_selng_amt,
    agrde_30_selng_amt,
    agrde_40_selng_amt,
    agrde_50_selng_amt,
    agrde_60_above_selng_amt,
    -- 매출 건수
    mdwk_selng_co,
    wkend_selng_co,
    mon_selng_co,
    tues_selng_co,
    wed_selng_co,
    thur_selng_co,
    fri_selng_co,
    sat_selng_co,
    sun_selng_co,
    tmzon_00_06_selng_co,
    tmzon_06_11_selng_co,
    tmzon_11_14_selng_co,
    tmzon_14_17_selng_co,
    tmzon_17_21_selng_co,
    tmzon_21_24_selng_co,
    -- 성별, 연령대별 매출 건수
    ml_selng_co,
    fml_selng_co,
    agrde_10_selng_co,
    agrde_20_selng_co,
    agrde_30_selng_co,
    agrde_40_selng_co,
    agrde_50_selng_co,
    agrde_60_above_selng_co

FROM staging_market_quarter_data