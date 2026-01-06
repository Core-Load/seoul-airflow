WITH source AS (
    SELECT * FROM {{ source('raw_data', '3Q_market_info') }}
),

seoul_3Q_market_info AS (
    SELECT
        CAST(CAST("STDR_YYQU_CD" AS NUMERIC) AS INT) AS stdr_yyqu_cd,
        "TRDAR_SE_CD" AS trdar_se_cd,
        "TRDAR_SE_CD_NM" AS trdar_se_cd_nm,
        CAST(CAST("TRDAR_CD" AS NUMERIC) AS INT) AS trdar_cd,
        "TRDAR_CD_NM" AS trdar_cd_nm,
        "SVC_INDUTY_CD" AS svc_induty_cd,
        "SVC_INDUTY_CD_NM" AS svc_induty_cd_nm,
        CAST(CAST("THSMON_SELNG_AMT" AS NUMERIC) AS BIGINT) AS thsmon_selng_amt,
        CAST(CAST("THSMON_SELNG_CO" AS NUMERIC) AS INT) AS thsmon_selng_co,
        CAST(CAST("MDWK_SELNG_AMT" AS NUMERIC) AS BIGINT) AS mdwk_selng_amt,
        CAST(CAST("WKEND_SELNG_AMT" AS NUMERIC) AS BIGINT) AS wkend_selng_amt,
        -- 요일 별 매출액
        CAST(CAST("MON_SELNG_AMT" AS NUMERIC) AS BIGINT) AS mon_selng_amt,
        CAST(CAST("TUES_SELNG_AMT" AS NUMERIC) AS BIGINT) AS tues_selng_amt,
        CAST(CAST("WED_SELNG_AMT" AS NUMERIC) AS BIGINT) AS wed_selng_amt,
        CAST(CAST("THUR_SELNG_AMT" AS NUMERIC) AS BIGINT) AS thur_selng_amt,
        CAST(CAST("FRI_SELNG_AMT" AS NUMERIC) AS BIGINT) AS fri_selng_amt,
        CAST(CAST("SAT_SELNG_AMT" AS NUMERIC) AS BIGINT) AS sat_selng_amt,
        CAST(CAST("SUN_SELNG_AMT" AS NUMERIC) AS BIGINT) AS sun_selng_amt,
        -- 시간대 별 매출액
        CAST(CAST("TMZON_00_06_SELNG_AMT" AS NUMERIC) AS BIGINT) AS tmzon_00_06_selng_amt,
        CAST(CAST("TMZON_06_11_SELNG_AMT" AS NUMERIC) AS BIGINT) AS tmzon_06_11_selng_amt,
        CAST(CAST("TMZON_11_14_SELNG_AMT" AS NUMERIC) AS BIGINT) AS tmzon_11_14_selng_amt,
        CAST(CAST("TMZON_14_17_SELNG_AMT" AS NUMERIC) AS BIGINT) AS tmzon_14_17_selng_amt,
        CAST(CAST("TMZON_17_21_SELNG_AMT" AS NUMERIC) AS BIGINT) AS tmzon_17_21_selng_amt,
        CAST(CAST("TMZON_21_24_SELNG_AMT" AS NUMERIC) AS BIGINT) AS tmzon_21_24_selng_amt,
        -- 성별, 연령대별 매출액
        CAST(CAST("ML_SELNG_AMT" AS NUMERIC) AS BIGINT) AS ml_selng_amt,
        CAST(CAST("FML_SELNG_AMT" AS NUMERIC) AS BIGINT) AS fml_selng_amt,
        CAST(CAST("AGRDE_10_SELNG_AMT" AS NUMERIC) AS BIGINT) AS agrde_10_selng_amt,
        CAST(CAST("AGRDE_20_SELNG_AMT" AS NUMERIC) AS BIGINT) AS agrde_20_selng_amt,
        CAST(CAST("AGRDE_30_SELNG_AMT" AS NUMERIC) AS BIGINT) AS agrde_30_selng_amt,
        CAST(CAST("AGRDE_40_SELNG_AMT" AS NUMERIC) AS BIGINT) AS agrde_40_selng_amt,
        CAST(CAST("AGRDE_50_SELNG_AMT" AS NUMERIC) AS BIGINT) AS agrde_50_selng_amt,
        CAST(CAST("AGRDE_60_ABOVE_SELNG_AMT" AS NUMERIC) AS BIGINT) AS agrde_60_above_selng_amt,
        -- 매출 건수
        CAST(CAST("MDWK_SELNG_CO" AS NUMERIC) AS INT) AS mdwk_selng_co,
        CAST(CAST("WKEND_SELNG_CO" AS NUMERIC) AS INT) AS wkend_selng_co,
        CAST(CAST("MON_SELNG_CO" AS NUMERIC) AS INT) AS mon_selng_co,
        CAST(CAST("TUES_SELNG_CO" AS NUMERIC) AS INT) AS tues_selng_co,
        CAST(CAST("WED_SELNG_CO" AS NUMERIC) AS INT) AS wed_selng_co,
        CAST(CAST("THUR_SELNG_CO" AS NUMERIC) AS INT) AS thur_selng_co,
        CAST(CAST("FRI_SELNG_CO" AS NUMERIC) AS INT) AS fri_selng_co,
        CAST(CAST("SAT_SELNG_CO" AS NUMERIC) AS INT) AS sat_selng_co,
        CAST(CAST("SUN_SELNG_CO" AS NUMERIC) AS INT) AS sun_selng_co,
        CAST(CAST("TMZON_00_06_SELNG_CO" AS NUMERIC) AS INT) AS tmzon_00_06_selng_co,
        CAST(CAST("TMZON_06_11_SELNG_CO" AS NUMERIC) AS INT) AS tmzon_06_11_selng_co,
        CAST(CAST("TMZON_11_14_SELNG_CO" AS NUMERIC) AS INT) AS tmzon_11_14_selng_co,
        CAST(CAST("TMZON_14_17_SELNG_CO" AS NUMERIC) AS INT) AS tmzon_14_17_selng_co,
        CAST(CAST("TMZON_17_21_SELNG_CO" AS NUMERIC) AS INT) AS tmzon_17_21_selng_co,
        CAST(CAST("TMZON_21_24_SELNG_CO" AS NUMERIC) AS INT) AS tmzon_21_24_selng_co,
        -- 성별, 연령대별 매출 건수
        CAST(CAST("ML_SELNG_CO" AS NUMERIC) AS INT) AS ml_selng_co,
        CAST(CAST("FML_SELNG_CO" AS NUMERIC) AS INT) AS fml_selng_co,
        CAST(CAST("AGRDE_10_SELNG_CO" AS NUMERIC) AS INT) AS agrde_10_selng_co,
        CAST(CAST("AGRDE_20_SELNG_CO" AS NUMERIC) AS INT) AS agrde_20_selng_co,
        CAST(CAST("AGRDE_30_SELNG_CO" AS NUMERIC) AS INT) AS agrde_30_selng_co,
        CAST(CAST("AGRDE_40_SELNG_CO" AS NUMERIC) AS INT) AS agrde_40_selng_co,
        CAST(CAST("AGRDE_50_SELNG_CO" AS NUMERIC) AS INT) AS agrde_50_selng_co,
        CAST(CAST("AGRDE_60_ABOVE_SELNG_CO" AS NUMERIC) AS INT) AS agrde_60_above_selng_co
    FROM source
)

SELECT * FROM seoul_3Q_market_info