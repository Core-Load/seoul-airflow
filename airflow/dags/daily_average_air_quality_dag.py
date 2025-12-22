from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from seoul_utils import SeoulAPI
from s3_utils import S3Manager
from db_utils import PostgreSqlManager
from slack import on_failure_callback

AWS_CONN_ID = "conn_aws"
POSTGRES_CONN_ID = "conn_postgres"
S3_BUCKET_NAME = Variable.get("s3_bucket_name")
SCHEMA_NAME = "raw_data"
TABLE_NAME = f"{SCHEMA_NAME}.daily_average_air_quality"
TARGET_DATE = (SeoulAPI.get_kst_now() - timedelta(days=1)).strftime('%Y%m%d')

def fetch_yesterday_air_quality():
    api = SeoulAPI()
    data = api.api_request(f"json/DailyAverageAirQuality/1/50/{TARGET_DATE}")
    return data

def save_data_to_s3(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="req_api")

    # S3 키 생성 (실시간 데이터이므로 target_date는 None)
    api_name = "서울시_일별_평균_대기오염도_정보"
    folder_name = "weather"
    target_date = TARGET_DATE + "0000"
    s3_key = SeoulAPI.generate_s3_key(api_name, folder_name, target_date)

    # S3에 업로드
    s3 = S3Manager(
        conn_id=AWS_CONN_ID,
        bucket_name=S3_BUCKET_NAME
    )
    s3.upload_json(key=s3_key, data=data)
    print(f"S3 저장 완료: s3://{S3_BUCKET_NAME}/{s3_key}")
    return s3_key

def create_table_if_not_exists(**context):
    db = PostgreSqlManager(conn_id=POSTGRES_CONN_ID)
    db.create_schema_if_not_exists(SCHEMA_NAME)

    table_name_only = TABLE_NAME.split('.')[-1]
    if db.table_exists(table_name_only, schema=SCHEMA_NAME):
        print(f"테이블이 이미 존재합니다: {TABLE_NAME}")
        return
    
    # 테이블 생성 쿼리
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            MSRMT_DT VARCHAR(8),
            MSRSTN_NM VARCHAR(50),
            NTDX NUMERIC(10, 4),
            OZON NUMERIC(10, 4),
            CBMX NUMERIC(10, 2),
            SPDX NUMERIC(10, 4),
            PM NUMERIC(10, 2),
            FPM NUMERIC(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (MSRMT_DT, MSRSTN_NM)
        )
    """
    db.create_table(create_query)
    print(f"테이블 생성 완료: {TABLE_NAME}")

def insert_data_to_postgres(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="req_api")
    db = PostgreSqlManager(conn_id=POSTGRES_CONN_ID)
    
    # 필터 함수: MSRMT_DT 또는 MSRSTN_NM이 없는 경우 제외
    def filter_valid_data(row):
        return bool(row.get("MSRMT_DT")) and bool(row.get("MSRSTN_NM"))
    
    # JSON 데이터 INSERT
    inserted_count = db.insert_from_json(
        table_name=TABLE_NAME,
        json_data=data,
        json_path=["DailyAverageAirQuality", "row"],  # JSON 내 데이터 경로
        filter_func=filter_valid_data,
        conflict_columns=["MSRMT_DT", "MSRSTN_NM"],
        update_columns=["NTDX", "OZON", "CBMX", "SPDX", "PM", "FPM", "updated_at"]
    )
    print(f"데이터 UPSERT 완료: {inserted_count}건")


with DAG(
    dag_id="daily_average_air_quality_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 10 * * *",     # 매일 오전 10시에 실행
    catchup=False,
    tags=["seoul", "weather"],
    on_failure_callback=on_failure_callback,
    default_args={
        "on_failure_callback": on_failure_callback
    }
) as dag:

    # 1. API 호출
    req_api = PythonOperator(
        task_id="req_api",
        python_callable=fetch_yesterday_air_quality
    )

    # 2. S3 저장
    save_file = PythonOperator(
        task_id="save_file",
        python_callable=save_data_to_s3
    )
    
    # 3. 스키마 및 테이블 생성 (없는 경우)
    create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table_if_not_exists
    )
    
    # 4. PostgreSQL에 데이터 INSERT
    insert_to_db = PythonOperator(
        task_id="insert_to_db",
        python_callable=insert_data_to_postgres
    )

    # Task 의존성 설정
    req_api >> save_file >> create_table >> insert_to_db