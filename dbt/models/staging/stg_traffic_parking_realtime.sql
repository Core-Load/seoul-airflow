-- stg_traffic_parking_realtime.sql
SELECT
    parking->>'PRK_NM' AS parking_name,
    parking->>'PRK_CD' AS parking_code,
    NULLIF(parking->>'CUR_PRK_CNT', '')::int AS available_spaces,
    (parking->>'CPCTY')::int AS total_capacity,
    (parking->>'LAT')::numeric AS lat,
    (parking->>'LNG')::numeric AS lng,
    NULLIF(parking->>'CUR_PRK_TIME', '')::timestamp AS observed_at,
    parking->>'CUR_PRK_YN' AS real_time_available,
    parking->>'PAY_YN' AS pay_yn
FROM raw_data.realtime_city_data
CROSS JOIN LATERAL
    jsonb_array_elements(
        data->'CITYDATA'->'PRK_STTS'
    ) AS parking
