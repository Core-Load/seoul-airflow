WITH source AS (
    SELECT
        area_name,
        created_at,
        data::jsonb AS data_json
    FROM {{ source('raw_data', 'realtime_city_data') }}
    WHERE created_at >= NOW() - INTERVAL '2 hours'
),
weather AS (
    SELECT
        s.area_name,
        s.created_at,
        w.value AS weather_json
    FROM source s
    CROSS JOIN LATERAL jsonb_array_elements(
        s.data_json
            -> 'CITYDATA'
            -> 'WEATHER_STTS'
    ) AS w(value)
    WHERE w.value IS NOT NULL
)
SELECT
    area_name,
    created_at,
    weather_json ->> 'WEATHER_TIME' AS weather_time,                -- 날씨 데이터 업데이트 시간
    (weather_json ->> 'TEMP')::numeric AS temp,                     -- 기온
    (weather_json ->> 'SENSIBLE_TEMP')::numeric AS sensible_temp,   -- 체감온도
    (weather_json ->> 'MAX_TEMP')::numeric AS max_temp,             -- 일 최고온도
    (weather_json ->> 'MIN_TEMP')::numeric AS min_temp,             -- 일 최저온도
    (weather_json ->> 'HUMIDITY')::numeric AS humidity,             -- 습도
    weather_json ->> 'WIND_DIRCT' AS wind_dirct,                    -- 풍향
    (weather_json ->> 'WIND_SPD')::numeric AS wind_spd,             -- 풍속
    weather_json ->> 'PRECIPITATION' AS precipitation,              -- 강수량
    weather_json ->> 'PRECPT_TYPE' AS precpt_type,                  -- 강수형태
    (weather_json ->> 'UV_INDEX_LVL')::numeric AS uv_index_lvl,     -- 자외선지수 단계
    weather_json ->> 'UV_INDEX' AS uv_index,                        -- 자외선지수
    weather_json ->> 'PM25_INDEX' AS pm25_index,                    -- 초미세먼지지표
    (weather_json ->> 'PM25')::numeric AS pm25,                     -- 초미세먼지농도
    weather_json ->> 'PM10_INDEX' AS pm10_index,                    -- 미세먼지지표
    (weather_json ->> 'PM10')::numeric AS pm10,                     -- 미세먼지농도
    weather_json ->> 'AIR_IDX' AS air_idx,                          -- 통합대기환경등급
    (weather_json ->> 'AIR_IDX_MVL')::numeric AS air_idx_mvl        -- 통합대기환경지수
FROM weather