-- dbt/models/staging/stg_road_traffic.sql
SELECT
    road->>'LINK_ID' AS link_id,
    road->>'ROAD_NM' AS road_name,
    road->>'START_ND_CD' AS start_nd_cd,
    road->>'START_ND_NM' AS start_nd_name,
    road->>'END_ND_CD' AS end_nd_cd,
    road->>'END_ND_NM' AS end_nd_name,
    NULLIF(road->>'DIST','')::numeric AS distance_m,
    NULLIF(road->>'SPD','')::numeric AS speed_kmh,
    road->>'IDX' AS congestion_status,
    road->>'XYLIST' AS xylist_raw
FROM raw_data.realtime_city_data,
LATERAL jsonb_array_elements(data->'CITYDATA'->'ROAD_TRAFFIC_STTS'->'ROAD_TRAFFIC_STTS') AS road;
