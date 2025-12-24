with recent as (
    select *
    from {{ ref('stg_realtime_city_weather') }}
    where
        WEATHER_TIME is not null
        and created_at >= now() - interval '1 hour'
),
ranked as (
    select
        *,
        row_number() over (
            partition by area_name
            order by
                weather_time desc,
                created_at desc
        ) as rn
    from recent
)
select
    area_name,
    created_at,
    WEATHER_TIME,   -- 날씨 데이터 업데이트 시간
    TEMP,           -- 기온
    SENSIBLE_TEMP,  -- 체감온도
    MAX_TEMP,       -- 일 최고온도
    MIN_TEMP,       -- 일 최저온도
    HUMIDITY,       -- 습도
    WIND_DIRCT,     -- 풍향
    WIND_SPD,       -- 풍속
    PRECIPITATION,  -- 강수량(mm 포함 텍스트)
    NULLIF(
        regexp_replace(PRECIPITATION, '[^0-9\.]', '', 'g'),
        ''
    )::numeric as PRECIPITATION_MM, -- 강수량(mm 제외 숫자)
    PRECPT_TYPE,    -- 강수형태
    UV_INDEX_LVL,   -- 자외선지수 단계
    UV_INDEX,       -- 자외선지수
    PM25_INDEX,     -- 초미세먼지지표
    PM25,           -- 초미세먼지농도
    PM10_INDEX,     -- 미세먼지지표
    PM10,           -- 미세먼지농도
    AIR_IDX,        -- 통합대기환경등급
    AIR_IDX_MVL     -- 통합대기환경지수
from ranked
where rn = 1