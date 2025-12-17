from __future__ import annotations
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from seoul_utils import SeoulAPI
from s3_utils import S3Manager
from db_utils import PostgresManager

AWS_CONN_ID = "conn_aws"
POSTGRES_CONN_ID = "conn_postgres"
S3_BUCKET_NAME = Variable.get("s3_bucket_name")
TABLE_NAME = "raw_data.realtime_city_air"


def fetch_seoul_air_quality():
    api = SeoulAPI()
    data = api.api_request("json/RealtimeCityAir/1/25/")
    return data

def save_data_to_s3(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="req_api")

    # S3 키 생성 (실시간 데이터이므로 target_date는 None)
    api_name = "서울시_권역별_실시간_대기환경_현황"
    s3_key = SeoulAPI.generate_s3_key(api_name=api_name, folder_name="weather")

    # S3에 업로드
    s3 = S3Manager(
        conn_id=AWS_CONN_ID,
        bucket_name=S3_BUCKET_NAME
    )
    
    s3.upload_json(key=s3_key, data=data)
    
    print(f"S3 저장 완료: s3://{S3_BUCKET_NAME}/{s3_key}")
    
    return s3_key

def create_table_if_not_exists(**context):
    db = PostgresManager(conn_id=POSTGRES_CONN_ID)
    
    # 테이블 존재 여부 확인
    if db.table_exists(TABLE_NAME):
        print(f"테이블이 이미 존재합니다: {TABLE_NAME}")
        return
    
    # 테이블 생성 쿼리
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            msrmt_dt VARCHAR(12),
            sarea_nm VARCHAR(50),
            msrstn_nm VARCHAR(50),
            pm NUMERIC(10, 2),
            fpm NUMERIC(10, 2),
            ozon NUMERIC(10, 3),
            ntdx NUMERIC(10, 3),
            cbmx NUMERIC(10, 2),
            spdx NUMERIC(10, 3),
            cai_grd VARCHAR(20),
            cai_idx NUMERIC(10, 2),
            crst_sbstn VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (msrmt_dt, msrstn_nm)
        )
    """
    
    db.create_table(create_query)
    print(f"테이블 생성 완료: {TABLE_NAME}")


def insert_data_to_postgres(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="req_api")
    
    db = PostgresManager(conn_id=POSTGRES_CONN_ID)
    
    # JSON 데이터에서 row 추출
    realtime_city_air = data.get("RealtimeCityAir", {})
    rows = realtime_city_air.get("row", [])
    
    if not rows:
        print("INSERT할 데이터가 없습니다")
        return
    
    # 데이터 변환 (컬럼명을 소문자로 변환)
    columns = [
        "msrmt_dt", "sarea_nm", "msrstn_nm", "pm", "fpm",
        "ozon", "ntdx", "cbmx", "spdx", "cai_grd",
        "cai_idx", "crst_sbstn"
    ]
    
    values = []
    for row in rows:
        # 빈 문자열이나 0.0 값을 가진 행은 제외 (측정소 오류)
        if not row.get("MSRSTN_NM") or row.get("PM", 0) == 0:
            continue
            
        value_tuple = (
            row.get("MSRMT_DT"),
            row.get("SAREA_NM"),
            row.get("MSRSTN_NM"),
            row.get("PM"),
            row.get("FPM"),
            row.get("OZON"),
            row.get("NTDX"),
            row.get("CBMX"),
            row.get("SPDX"),
            row.get("CAI_GRD"),
            row.get("CAI_IDX"),
            row.get("CRST_SBSTN")
        )
        values.append(value_tuple)
    
    # UPSERT 쿼리 (중복 시 업데이트)
    upsert_query = f"""
        INSERT INTO {TABLE_NAME} (
            {', '.join(columns)}
        ) VALUES ({', '.join(['%s'] * len(columns))})
        ON CONFLICT (msrmt_dt, msrstn_nm)
        DO UPDATE SET
            sarea_nm = EXCLUDED.sarea_nm,
            pm = EXCLUDED.pm,
            fpm = EXCLUDED.fpm,
            ozon = EXCLUDED.ozon,
            ntdx = EXCLUDED.ntdx,
            cbmx = EXCLUDED.cbmx,
            spdx = EXCLUDED.spdx,
            cai_grd = EXCLUDED.cai_grd,
            cai_idx = EXCLUDED.cai_idx,
            crst_sbstn = EXCLUDED.crst_sbstn,
            created_at = CURRENT_TIMESTAMP
    """
    
    # 배치 INSERT 실행
    conn = db.hook.get_conn()
    cursor = conn.cursor()
    
    try:
        for value in values:
            cursor.execute(upsert_query, value)
        conn.commit()
        print(f"데이터 INSERT 완료: {len(values)}건")
    except Exception as e:
        conn.rollback()
        print(f"데이터 INSERT 실패: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id="realtime_city_air_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0,30 * * * *",   # 매 시각 00분, 30분
    catchup=False,
    tags=["seoul", "weather", "postgres"]
) as dag:

    # 1. API 호출
    req_api = PythonOperator(
        task_id="req_api",
        python_callable=fetch_seoul_air_quality
    )

    # 2. S3 저장
    save_file = PythonOperator(
        task_id="save_file",
        python_callable=save_data_to_s3
    )
    
    # 3. 테이블 생성 (없는 경우)
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