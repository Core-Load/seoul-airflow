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
    WEATHER_TIME,
    TEMP,
    SENSIBLE_TEMP,
    MAX_TEMP,
    MIN_TEMP,
    HUMIDITY,
    WIND_DIRCT,
    WIND_SPD,
    PRECIPITATION,
    NULLIF(
        regexp_replace(PRECIPITATION, '[^0-9\.]', '', 'g'),
        ''
    )::numeric as PRECIPITATION_MM,
    PRECPT_TYPE,
    UV_INDEX_LVL,
    UV_INDEX,
    PM25_INDEX,
    PM25,
    PM10_INDEX,
    PM10,
    AIR_IDX,
    AIR_IDX_MVL
from ranked
where rn = 1