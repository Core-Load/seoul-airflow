with source as (
    select
        area_name,
        created_at,
        data::jsonb as data_json
    from raw_data.realtime_city_data
),
weather as (
    select
        s.area_name,
        s.created_at,
        w.value as weather_json
    from source s
    cross join lateral jsonb_array_elements(
        s.data_json
            -> 'CITYDATA'
            -> 'WEATHER_STTS'
    ) as w(value)
)
select
    area_name,
    created_at,
    weather_json ->> 'WEATHER_TIME'         as WEATHER_TIME,    -- 날씨 데이터 업데이트 시간
    weather_json ->> 'TEMP'                 as TEMP,            -- 기온
    weather_json ->> 'SENSIBLE_TEMP'        as SENSIBLE_TEMP,   -- 체감온도
    weather_json ->> 'MAX_TEMP'             as MAX_TEMP,        -- 일 최고온도
    weather_json ->> 'MIN_TEMP'             as MIN_TEMP,        -- 일 최저온도
    weather_json ->> 'HUMIDITY'             as HUMIDITY,        -- 습도
    weather_json ->> 'WIND_DIRCT'           as WIND_DIRCT,      -- 풍향
    weather_json ->> 'WIND_SPD'             as WIND_SPD,        -- 풍속
    weather_json ->> 'PRECIPITATION'        as PRECIPITATION,   -- 강수량
    weather_json ->> 'PRECPT_TYPE'          as PRECPT_TYPE,     -- 강수형태
    weather_json ->> 'UV_INDEX_LVL'         as UV_INDEX_LVL,    -- 자외선지수 단계
    weather_json ->> 'UV_INDEX'             as UV_INDEX,        -- 자외선지수
    weather_json ->> 'PM25_INDEX'           as PM25_INDEX,      -- 초미세먼지지표
    weather_json ->> 'PM25'                 as PM25,            -- 초미세먼지농도
    weather_json ->> 'PM10_INDEX'           as PM10_INDEX,      -- 미세먼지지표
    weather_json ->> 'PM10'                 as PM10,            -- 미세먼지농도
    weather_json ->> 'AIR_IDX'              as AIR_IDX,         -- 통합대기환경등급
    weather_json ->> 'AIR_IDX_MVL'          as AIR_IDX_MVL,     -- 통합대기환경지수
    weather_json                            as weather_raw
from weather