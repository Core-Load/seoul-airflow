select
    area_name,
    WEATHER_TIME,       -- 날씨 데이터 업데이트 시간
    TEMP,               -- 기온
    SENSIBLE_TEMP,      -- 체감온도
    MAX_TEMP,           -- 일 최고온도
    MIN_TEMP,           -- 일 최저온도
    HUMIDITY,           -- 습도
    WIND_DIRCT,         -- 풍향
    WIND_SPD,           -- 풍속
    PRECIPITATION,      -- 강수량(텍스트)
    PRECIPITATION_MM,   -- 강수량(숫자)
    PRECPT_TYPE,        -- 강수형태
    UV_INDEX_LVL,       -- 자외선지수단계
    UV_INDEX,           -- 자외선지수
    PM25_INDEX,         -- 초미세먼지지표
    PM25,               -- 초미세먼지농도
    PM10_INDEX,         -- 미세먼지지표
    PM10,               -- 미세먼지농도
    AIR_IDX,            -- 통합대기환경등급
    AIR_IDX_MVL,        -- 통합대기환경지수
    CURRENT_TIMESTAMP as snapshot_at
from {{ ref('int_city_weather') }}