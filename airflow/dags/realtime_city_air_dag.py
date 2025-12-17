from __future__ import annotations
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from seoul_utils import SeoulAPI
from s3_utils import S3Manager

AWS_CONN_ID = "conn_aws"
S3_BUCKET_NAME = Variable.get("s3_bucket_name")


def fetch_seoul_air_quality():
    api = SeoulAPI()
    data = api.api_request("json/RealtimeCityAir/1/25/")
    return data

def save_data_to_s3(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="req_api")

    # S3 키 생성 (실시간 데이터이므로 target_date는 None)
    api_name = "서울시_권역별_실시간_대기환경_현황"
    s3_key = SeoulAPI.generate_s3_key(api_name=api_name)

    # S3에 업로드
    s3 = S3Manager(
        conn_id=AWS_CONN_ID,
        bucket_name=S3_BUCKET_NAME
    )
    
    s3.upload_json(key=s3_key, data=data)
    
    print(f"S3 저장 완료: s3://{S3_BUCKET_NAME}/{s3_key}")


with DAG(
    dag_id="realtime_city_air_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["seoul", "weather"]
) as dag:

    # API 호출
    req_api = PythonOperator(
        task_id="req_api",
        python_callable=fetch_seoul_air_quality
    )

    # S3 저장
    save_file = PythonOperator(
        task_id="save_file",
        python_callable=save_data_to_s3
    )

    req_api >> save_file