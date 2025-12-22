SELECT
    area_name,
    -- 상권 공통 지표 (1:1 데이터)
    data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'AREA_CMRCL_LVL' AS commerce_lvl,       -- 상권 활성도
    (data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_MALE_RATE')::float AS female_rate,    -- 여성 결제 비중
    (data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_FEMALE_RATE')::float AS male_rate,        -- 남성 결제 비중
    (data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_10_RATE')::float AS age_10_rate,    -- 10대 결제 비중
    (data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_20_RATE')::float AS age_20_rate,    -- 20대 결제 비중
    (data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_30_RATE')::float AS age_30_rate,    -- 30대 결제 비중
    (data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_40_RATE')::float AS age_40_rate,    -- 40대 결제 비중
    (data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_50_RATE')::float AS age_50_rate,    -- 50대 결제 비중
    (data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_60_RATE')::float AS age_60_rate,    -- 60대 결제 비중
    (data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_PERSONAL_RATE')::float AS personal_rate,    -- 개인 결제 비중
    (data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_CORPORATION_RATE') AS corporation_rate,    -- 법인 결제 비중
    (data -> 'CITYDATA' -> 'LIVE_CMRCL_STTS' ->> 'CMRCL_TIME', 'YYYYMMDD HH24MI') AS commerce_time,    -- 결제 시간
    created_at AS loaded_at     -- stg_market_store와 join에 사용할 수 있는
FROM raw_data.realtime_city_data