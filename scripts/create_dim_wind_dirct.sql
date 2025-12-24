create table if not exists analytics.dim_wind_dirct (
    wind_dir    varchar(3)  primary key,
    wind_dir_kr varchar(10),
    wind_degree numeric(5,1),
    sort_order  smallint
);

insert into analytics.dim_wind_dirct (
    wind_dir,
    wind_dir_kr,
    wind_degree,
    sort_order
)
values
    ('N', '북', 0.0, 1),
    ('NNE', '북북동', 22.5, 2),
    ('NE', '북동', 45.0, 3),
    ('ENE', '동북동', 67.5, 4),
    ('E', '동', 90.0, 5),
    ('ESE', '동남동', 112.5, 6),
    ('SE', '남동', 135.0, 7),
    ('SSE', '남남동', 157.5, 8),
    ('S', '남', 180.0, 9),
    ('SSW', '남남서', 202.5, 10),
    ('SW', '남서', 225.0, 11),
    ('WSW', '서남서', 247.5, 12),
    ('W', '서', 270.0, 13),
    ('WNW', '서북서', 292.5, 14),
    ('NW', '북서', 315.0, 15),
    ('NNW', '북북서', 337.5, 16);