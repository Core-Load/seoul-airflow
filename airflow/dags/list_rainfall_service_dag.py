from __future__ import annotations
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
from airflow.utils import timezone
import pendulum
from seoul_utils import SeoulAPI
from s3_utils import S3Manager
from db_utils import PostgreSqlManager
from slack import on_failure_callback

AWS_CONN_ID = "conn_aws"
POSTGRES_CONN_ID = "conn_postgres"
S3_BUCKET_NAME = Variable.get("s3_bucket_name")
SCHEMA_NAME = "raw_data"
TABLE_NAME = f"{SCHEMA_NAME}.list_rainfall_service"

def skip_at_21(**context):
    now = pendulum.now("Asia/Seoul")
    return now.hour != 21

def fetch_yesterday_air_quality():
    api = SeoulAPI()
    data = api.api_request(f"json/ListRainfallService/1/50/")
    return data

def save_data_to_s3(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="req_api")

    # S3 키 생성
    api_name = "서울시_강우량_정보"
    folder_name = "weather"
    s3_key = SeoulAPI.generate_s3_key(api_name, folder_name)

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
            RF_CD INTEGER,
            RF_NM VARCHAR(50),
            GU_CD INTEGER,
            GU_NM VARCHAR(20),
            RN_10M NUMERIC(6, 2),
            DATA_CLCT_TM TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (RF_CD, DATA_CLCT_TM)
        );
        CREATE INDEX IF NOT EXISTS idx_lrs_rf_cd_updated_at
        ON {TABLE_NAME} (RF_CD, updated_at DESC);
    """
    db.create_table(create_query)
    print(f"테이블 생성 완료: {TABLE_NAME}")

def insert_data_to_postgres(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="req_api")
    db = PostgreSqlManager(conn_id=POSTGRES_CONN_ID)
    
    # 필터 함수: PRIMARY KEY 확인, 데이터 타입 변경
    def filter_valid_data(row):
        if not row.get("RF_CD") or not row.get("DATA_CLCT_TM"):
            return False

        # RN_10M: str → float
        try:
            row["RN_10M"] = float(row["RN_10M"])
        except (TypeError, ValueError):
            row["RN_10M"] = None

        # DATA_CLCT_TM: str → datetime
        try:
            row["DATA_CLCT_TM"] = datetime.strptime(
                row["DATA_CLCT_TM"], "%Y-%m-%d %H:%M"
            )
        except (TypeError, ValueError):
            return False

        return True
    
    # JSON 데이터 INSERT
    inserted_count = db.insert_from_json(
        table_name=TABLE_NAME,
        json_data=data,
        json_path=["ListRainfallService", "row"],  # JSON 내 데이터 경로
        filter_func=filter_valid_data,
        conflict_columns=["RF_CD", "DATA_CLCT_TM"],
        update_columns=["RF_NM", "GU_CD", "GU_NM", "RN_10M", "updated_at"]
    )
    print(f"데이터 UPSERT 완료: {inserted_count}건")


with DAG(
    dag_id="list_rainfall_service_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/10 * * * *",   # 10분마다 실행
    catchup=False,
    tags=["seoul", "weather", "s3", "postgres"],
    on_failure_callback=on_failure_callback,
    default_args={
        "on_failure_callback": on_failure_callback
    }
) as dag:
    # 서버 중지 시간 체크
    check_time = ShortCircuitOperator(
        task_id="check_not_21",
        python_callable=skip_at_21
    )

    # API 호출
    req_api = PythonOperator(
        task_id="req_api",
        python_callable=fetch_yesterday_air_quality
    )

    # S3 저장
    save_file = PythonOperator(
        task_id="save_file",
        python_callable=save_data_to_s3
    )
    
    # 스키마 및 테이블 생성 (없는 경우)
    create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table_if_not_exists
    )
    
    # PostgreSQL에 데이터 INSERT
    insert_to_db = PythonOperator(
        task_id="insert_to_db",
        python_callable=insert_data_to_postgres
    )

    # Task 의존성 설정
    check_time >> req_api >> save_file >> create_table >> insert_to_db