## 폴더 구조
    .
    └── dbt/
        ├── macros/
        │   └── schema.sql      # 스키마 생성 매크로
        │
        ├── models/
        │   ├── staging/        # 데이터 정제
        │   │   ├── sources.yml
        │   │   ├── stg_daily_air_quality.sql
        │   │   ├── stg_list_rainfall_service.sql
        │   │   ├── stg_market_3q_info.sql
        │   │   ├── stg_market_ppl.sql
        │   │   ├── stg_market_store.sql
        │   │   ├── stg_realtime_city_weather.sql
        │   │   ├── stg_traffic_parking_realtime.sql
        │   │   └── stg_traffic_road.sql
        │   │
        │   ├── intermediate/   # 비즈니스 로직 처리
        │   │   ├── int_air_quality.sql
        │   │   ├── int_city_weather.sql
        │   │   ├── int_market_ppl.sql
        │   │   ├── int_market_quarter.sql
        │   │   ├── int_market_store.sql
        │   │   ├── int_rainfall.sql
        │   │   ├── int_traffic_parking_realtime.sql
        │   │   └── int_traffic_road.sql
        │   │
        │   └── marts/          # 최종 분석용 모델
        │       ├── dim_market_quarter.sql
        │       ├── dim_rainfall.sql
        │       ├── fact_air_quality.sql
        │       ├── fact_city_weather.sql
        │       ├── fact_market_ppl.sql
        │       ├── fact_market_quarter.sql
        │       ├── fact_market_store.sql
        │       ├── fact_rainfall.sql
        │       ├── fact_traffic_parking_map.sql
        │       └── fact_traffic_road_congestion.sql
        │
        ├── dbt_project.yml
        ├── profiles.yml
        ├── Dockerfile
        └── requirements.txt