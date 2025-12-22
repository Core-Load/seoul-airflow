SELECT
    area_name,
    -- 상권 공통 지표 (1:1 데이터)
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'AREA_CMRCL_LVL' AS commerce_lvl,       -- 상권 활성도
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_MALE_RATE' AS female_rate,    -- 여성 결제 비중
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_FEMALE_RATE' AS male_rate,        -- 남성 결제 비중
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_10_RATE' AS age_20_rate,    -- 20대 결제 비중
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_20_RATE' AS age_30_rate,    -- 30대 결제 비중
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_30_RATE' AS age_30_rate,    -- 30대 결제 비중
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_40_RATE' AS age_30_rate,    -- 30대 결제 비중
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_50_RATE' AS age_30_rate,    -- 30대 결제 비중
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_60_RATE' AS age_30_rate,    -- 30대 결제 비중
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_PERSONAL_RATE' AS age_30_rate,    -- 30대 결제 비중
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_CORPORATION_RATE' AS age_30_rate,    -- 30대 결제 비중
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_TIME' AS age_30_rate,    -- 30대 결제 비중
    created_at      -- stg_market_store와 join에 사용할 수 있는
FROM raw_data.realtime_city_data;