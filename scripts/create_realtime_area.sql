create table if not exists analytics.dim_realtime_area (
    area_key            integer primary key,
    area_name           text not null,
    area_display_name   text not null,
    latitude            numeric(9,6) not null,
    longitude           numeric(9,6) not null,
    area_type           text not null,
    is_active           boolean not null default true,
    created_at          timestamp not null default now()
);

create unique index if not exists idx_dim_realtime_area_name
on analytics.dim_realtime_area (area_name);

insert into analytics.dim_realtime_area (
    area_key,
    area_name,
    area_display_name,
    latitude,
    longitude,
    area_type,
    is_active
)
values
    (1, '명동 관광특구', '명동 관광특구', 37.563617, 126.982677, '관광특구', true),
    (2, '광화문·덕수궁', '광화문·덕수궁', 37.571648, 126.976937, '관광지', true),
    (3, '어린이대공원', '어린이대공원', 37.547707, 127.074707, '공원', true),
    (4, '여의도', '여의도', 37.521941, 126.924579, '업무지구', true),
    (5, '성수카페거리', '성수 카페거리', 37.544581, 127.055961, '상권', true),
    (6, '홍대입구역(2호선)', '홍대입구', 37.557192, 126.924916, '상권', true),
    (7, '강남역', '강남역', 37.497942, 127.027621, '상권', true),
    (8, 'DMC(디지털미디어시티)', 'DMC', 37.577528, 126.889543, '업무지구', true),
    (9, '광장(전통)시장', '광장시장', 37.570047, 126.999889, '전통시장', true),
    (10, '서울식물원·마곡나루역', '서울식물원·마곡나루역', 37.568403, 126.829483, '공원', true)
on conflict (area_key) do nothing;