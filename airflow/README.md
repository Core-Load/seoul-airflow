## 폴더 구조
    .
    └── airflow/
        ├── dags/
        │   ├── sql/    # Airflow에서 사용하는 SQL
        │   │   ├── create_daily_average_air_quality.sql
        │   │   ├── create_list_rainfall_service.sql
        │   │   ├── create_realtime_city_data.sql
        │   │   └── market_info_setup.sql
        │   │
        │   ├── daily_average_air_quality_dag.py        # 일별 평균 대기오염도 데이터 수집
        │   ├── list_rainfall_service_dag.py            # 강우량 데이터 수집
        │   ├── quarter_market_dag.py
        │   ├── seoul_citydata_to_s3_dag.py
        │   ├── dbt_daily_average_air_quality_dag.py    # 일별 평균 대기오염도 관련 dbt 실행
        │   ├── dbt_list_rainfall_service_dag.py        # 강우량 관련 dbt 실행
        │   ├── dbt_market_dag.py
        │   ├── dbt_market_quarter_dag.py
        │   ├── dbt_realtime_city_weather_dag.py        # 실시간 날씨 관련 dbt 실행
        │   └── dbt_traffic_dag.py
        │
        ├── plugins/    # 공통 유틸 및 외부 연동 모듈
        │   ├── common_utils.py
        │   ├── db_utils.py
        │   ├── s3_utils.py
        │   ├── seoul_utils.py
        │   └── slack.py
        │
        ├── Dockerfile
        └── requirements.txt